// Command ingestor connects Polymarket (Gamma REST + CLOB WebSocket) to
// Kafka, per design-plan.md section 4.1: market discovery, WS sharding, and
// a bounded in-memory buffer so a slow/unavailable Kafka never blocks the WS
// read loop.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nawat-john/oddspulse/internal/kafka"
	"github.com/nawat-john/oddspulse/internal/metrics"
	"github.com/nawat-john/oddspulse/internal/model"
	"github.com/nawat-john/oddspulse/internal/polymarket/clobws"
	"github.com/nawat-john/oddspulse/internal/polymarket/gamma"
	"github.com/nawat-john/oddspulse/internal/shutdown"
)

// Config is the ingestor's environment configuration (design-plan.md
// section 13).
type Config struct {
	KafkaBrokers  []string `env:"KAFKA_BROKERS" envDefault:"localhost:19092" envSeparator:","`
	GammaURL      string   `env:"PM_GAMMA_URL" envDefault:"https://gamma-api.polymarket.com"`
	WSURL         string   `env:"PM_WS_URL" envDefault:"wss://ws-subscriptions-clob.polymarket.com/ws/market"`
	MaxAssets     int      `env:"PM_MAX_ASSETS" envDefault:"1000"`
	AssetsPerConn int      `env:"PM_ASSETS_PER_CONN" envDefault:"200"`
	MetricsAddr   string   `env:"METRICS_ADDR" envDefault:":9090"`
}

// refreshInterval is the market-discovery poll period (design-plan.md 4.1:
// "every 5 minutes"). Not exposed as an env var - the design plan gives one
// fixed value and no reason to tune it per deployment.
const refreshInterval = 5 * time.Minute

// bufferSize bounds the in-memory queue between the WS read loop and the
// Kafka producer (design-plan.md 4.1: bounded buffer, drop-with-metric,
// never block the WS read). Not exposed as an env var for the same reason
// as refreshInterval.
const bufferSize = 100_000

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "ingestor")

	cfg, err := env.ParseAs[Config]()
	if err != nil {
		log.Error("config", "error", err)
		os.Exit(1)
	}

	ctx, stop := shutdown.NewContext()
	defer stop()

	go func() {
		log.Info("metrics listening", "addr", cfg.MetricsAddr)
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "error", err)
		}
	}()

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Error("kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	buf := make(chan model.RawEvent, bufferSize)

	// Drains the bounded buffer and produces to pm.raw; never blocks the WS
	// read loop (design-plan.md 4.1).
	go runProducerLoop(ctx, log, producer, buf)

	onEvents := func(events []model.RawEvent) {
		for _, ev := range events {
			select {
			case buf <- ev:
			default:
				metrics.IngestorDroppedTotal.Inc()
			}
		}
	}

	wsCfg := clobws.DefaultConfig(cfg.WSURL, cfg.AssetsPerConn)
	wsManager := clobws.NewManager(wsCfg, log, metrics.IngestorAdapter{}, onEvents)
	defer wsManager.Stop()

	gammaClient := gamma.NewClient(cfg.GammaURL)
	runMarketRefreshLoop(ctx, log, gammaClient, producer, wsManager, cfg.MaxAssets)

	log.Info("shutting down")
}

// runMarketRefreshLoop fetches active markets from Gamma every
// refreshInterval (and once immediately at startup), produces pm.markets,
// and updates the WS subscription set. It returns once ctx is done.
func runMarketRefreshLoop(
	ctx context.Context,
	log *slog.Logger,
	gammaClient *gamma.Client,
	producer *kgo.Client,
	wsManager *clobws.Manager,
	maxAssets int,
) {
	t := time.NewTicker(refreshInterval)
	defer t.Stop()

	refresh := func() {
		markets, err := gammaClient.FetchActiveMarkets(ctx)
		if err != nil {
			log.Warn("market refresh: fetch failed", "error", err)
			return
		}
		top := gamma.TopNByVolume(markets, maxAssets)

		var assetIDs []string
		for _, m := range top {
			produceMarket(ctx, log, producer, m)
			assetIDs = append(assetIDs, m.AssetIDs()...)
		}
		log.Info("market refresh", "markets", len(top), "assets", len(assetIDs))
		wsManager.SetAssets(ctx, assetIDs)
	}

	refresh()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			refresh()
		}
	}
}

func produceMarket(ctx context.Context, log *slog.Logger, p *kgo.Client, m model.Market) {
	value, err := json.Marshal(m)
	if err != nil {
		log.Warn("market refresh: marshal market", "market_id", m.MarketID, "error", err)
		return
	}
	kafka.Produce(ctx, p, "pm.markets", []byte(m.MarketID), value, func(err error) {
		if err != nil {
			metrics.IngestorProduceErrorsTotal.Inc()
			log.Warn("produce pm.markets failed", "market_id", m.MarketID, "error", err)
		}
	})
}

// runProducerLoop drains buf and produces each event to pm.raw, keyed by
// asset_id (design-plan.md 4.1: same-outcome events land in the same
// partition, preserving order).
func runProducerLoop(ctx context.Context, log *slog.Logger, p *kgo.Client, buf <-chan model.RawEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-buf:
			value, err := json.Marshal(ev)
			if err != nil {
				log.Warn("produce pm.raw: marshal event", "asset_id", ev.AssetID, "error", err)
				continue
			}
			kafka.Produce(ctx, p, "pm.raw", []byte(ev.AssetID), value, func(err error) {
				if err != nil {
					metrics.IngestorProduceErrorsTotal.Inc()
				}
			})
		}
	}
}
