// Command gateway fans out pm.ticks/pm.alerts/pm.top to WebSocket clients
// (design-plan.md section 4.3): the whole benchmark story rests on this
// service. Kafka consumption/fan-out logic lives in internal/hub; this
// command wires it to a real Kafka consumer and a real WS server.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nawat-john/oddspulse/internal/hub"
	"github.com/nawat-john/oddspulse/internal/kafka"
	"github.com/nawat-john/oddspulse/internal/metrics"
	"github.com/nawat-john/oddspulse/internal/shutdown"
)

const (
	topicTicks     = "pm.ticks"
	topicAlerts    = "pm.alerts"
	topicTop       = "pm.top"
	topicSnapshots = "pm.snapshots"
	topicMarkets   = "pm.markets"
)

// latencySampleCapacity bounds the in-memory ring buffer runSysLoop's p99
// is computed from. Not exposed as an env var - an implementation detail of
// the sys-channel display, not a deployment tunable.
const latencySampleCapacity = 4096

// closeCodeServiceRestart is RFC 6455 close code 1012, sent to every client
// on graceful shutdown (design-plan.md 4.3) so they reconnect elsewhere.
const closeCodeServiceRestart = 1012

// bootstrapTimeout is a defensive upper bound on the whole bootstrap step
// (pm.snapshots + pm.markets, bootstrap.go). Normally this completes in
// well under a second per topic.
const bootstrapTimeout = 60 * time.Second

// Config is the gateway's environment configuration (design-plan.md section
// 13).
type Config struct {
	KafkaBrokers   []string `env:"KAFKA_BROKERS" envDefault:"localhost:19092" envSeparator:","`
	FlushMS        int      `env:"GW_FLUSH_MS" envDefault:"100"`
	MaxSubs        int      `env:"GW_MAX_SUBS" envDefault:"500"`
	AllowedOrigins []string `env:"GW_ALLOWED_ORIGINS" envSeparator:"," envDefault:"http://localhost:5173"`
	MetricsAddr    string   `env:"METRICS_ADDR" envDefault:":9090"`
	Addr           string   `env:"GW_ADDR" envDefault:":8080"`
}

// gatewayServer holds everything the HTTP handlers (ws.go) and the
// Kafka/sys loops (sys.go, bootstrap.go) share.
type gatewayServer struct {
	cfg Config
	hub *hub.Hub
	log *slog.Logger

	appCtx context.Context

	markets  *marketCache
	latency  *latencySketch
	inEvents atomic.Int64

	bootstrapped atomic.Bool
	consuming    atomic.Bool
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "gateway")

	cfg, err := env.ParseAs[Config]()
	if err != nil {
		log.Error("config", "error", err)
		os.Exit(1)
	}

	ctx, stop := shutdown.NewContext()
	defer stop()

	// Metrics-only server, matching ingestor/processor's convention.
	go func() {
		log.Info("metrics listening", "addr", cfg.MetricsAddr)
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "error", err)
		}
	}()

	h := hub.NewHub(hub.Config{
		FlushInterval: time.Duration(cfg.FlushMS) * time.Millisecond,
		MaxSubs:       cfg.MaxSubs,
		ServerName:    serverName(),
	}, metrics.GatewayAdapter{}, log)

	gw := &gatewayServer{
		cfg:     cfg,
		hub:     h,
		log:     log,
		appCtx:  ctx,
		markets: newMarketCache(),
		latency: newLatencySketch(latencySampleCapacity),
	}

	// Bounded even though consumeUpToEnd (bootstrap.go) targets a fixed
	// offset snapshot rather than an open-ended idle heuristic: a defensive
	// upper bound in case Kafka is unreachable or a topic is unexpectedly
	// huge.
	bootstrapCtx, bootstrapCancel := context.WithTimeout(ctx, bootstrapTimeout)
	err = gw.bootstrap(bootstrapCtx, cfg.KafkaBrokers)
	bootstrapCancel()
	if err != nil {
		log.Error("bootstrap", "error", err)
		os.Exit(1)
	}
	gw.bootstrapped.Store(true)

	// Unique consumer group per instance: every gateway instance sees every
	// partition (design-plan.md 4.3's "broadcast" pattern, ADR-002).
	groupID := fmt.Sprintf("gateway-%s-%d", serverName(), rand.Uint64())
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, groupID, []string{topicTicks, topicAlerts, topicTop},
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		log.Error("kafka consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	go h.Run(ctx)
	go gw.runSysLoop(ctx)

	gw.consuming.Store(true)
	go func() {
		gw.runConsumeLoop(ctx, consumer)
		gw.consuming.Store(false)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", gw.handleWS)
	mux.HandleFunc("/healthz", gw.handleHealthz)
	mux.HandleFunc("/readyz", gw.handleReadyz)
	mux.HandleFunc("/markets", gw.handleMarkets)
	srv := &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("ws listening", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "error", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	// Design-plan.md 4.3: send every client a close frame with code 1012 so
	// they reconnect to another instance, before this one goes away.
	h.CloseAll(closeCodeServiceRestart, "service restart")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = srv.Shutdown(shutdownCtx)
	cancel()
}

func (gw *gatewayServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// handleReadyz reports ready once the startup caches are built and the
// pm.ticks/pm.alerts/pm.top consume loop is actively running (design-plan.md
// 4.3: "readiness = consumer caught up and cache ready").
func (gw *gatewayServer) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if gw.bootstrapped.Load() && gw.consuming.Load() {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

func serverName() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "gateway"
}
