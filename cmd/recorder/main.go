// Command recorder consumes pm.ticks and pm.alerts and writes them as
// gzip-compressed NDJSON to data/replay/YYYY-MM-DD/HH.ndjson.gz, rotating on
// the hour (design-plan.md section 4.4). scripts/replay-export.sh later
// trims a window of these files for web/public/replay/'s Replay Mode
// (design-plan.md section 8).
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
	"github.com/nawat-john/oddspulse/internal/shutdown"
)

const (
	topicTicks  = "pm.ticks"
	topicAlerts = "pm.alerts"

	consumerGroup = "recorder"
)

// Config is the recorder's environment configuration (design-plan.md
// section 13).
type Config struct {
	KafkaBrokers []string `env:"KAFKA_BROKERS" envDefault:"localhost:19092" envSeparator:","`
	ReplayDir    string   `env:"REPLAY_DIR" envDefault:"data/replay"`
	MetricsAddr  string   `env:"METRICS_ADDR" envDefault:":9090"`
}

// record is the NDJSON line schema: which channel a line came from plus its
// original JSON payload verbatim (a model.Tick or model.Alert), so
// ReplaySource can derive replay pacing from the payload's own rts/pts/ts
// fields without the recorder duplicating them.
type record struct {
	T string          `json:"t"` // "tick" | "alert"
	D json.RawMessage `json:"d"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "recorder")

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

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, consumerGroup, []string{topicTicks, topicAlerts},
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		log.Error("kafka consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	w := newRotatingWriter(cfg.ReplayDir)
	defer func() {
		if err := w.Close(); err != nil {
			log.Warn("close replay file", "error", err)
		}
	}()

	log.Info("starting", "dir", cfg.ReplayDir)
	runConsumeLoop(ctx, log, consumer, w)
	log.Info("shutting down")
}

func runConsumeLoop(ctx context.Context, log *slog.Logger, consumer *kgo.Client, w *rotatingWriter) {
	for {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("fetch error", "error", err)
		}
		if fetches.Empty() {
			continue
		}

		now := time.Now()
		fetches.EachRecord(func(r *kgo.Record) {
			kind := "tick"
			if r.Topic == topicAlerts {
				kind = "alert"
			}
			line, err := json.Marshal(record{T: kind, D: r.Value})
			if err != nil {
				log.Warn("marshal record", "error", err)
				metrics.RecorderWriteErrorsTotal.Inc()
				return
			}
			if err := w.WriteLine(now, line); err != nil {
				log.Warn("write replay file", "error", err)
				metrics.RecorderWriteErrorsTotal.Inc()
				return
			}
			metrics.RecorderRecordsWrittenTotal.WithLabelValues(r.Topic).Inc()
		})

		if err := w.Flush(); err != nil {
			log.Warn("flush replay file", "error", err)
			metrics.RecorderWriteErrorsTotal.Inc()
		}
		if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
			log.Warn("commit offsets", "error", err)
		}
	}
}
