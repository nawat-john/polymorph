// Command processor consumes pm.raw, maintains per-asset state via
// internal/stats, and produces pm.ticks/pm.alerts/pm.snapshots/pm.top
// (design-plan.md section 4.2).
//
// Scaling note (design-plan.md 4.2): this uses a single consumer group,
// "processor". State is keyed by asset_id and pm.raw is keyed the same way,
// so Kafka guarantees every asset's records land on one partition - running
// several processor instances in the same group (up to pm.raw's partition
// count) works correctly today with zero code changes: each instance simply
// owns a disjoint subset of assets, and the bootstrap-from-pm.snapshots /
// at-least-once produce-then-commit logic below applies per instance.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nawat-john/oddspulse/internal/kafka"
	"github.com/nawat-john/oddspulse/internal/metrics"
	"github.com/nawat-john/oddspulse/internal/model"
	"github.com/nawat-john/oddspulse/internal/shutdown"
	"github.com/nawat-john/oddspulse/internal/stats"
)

const (
	topicRaw       = "pm.raw"
	topicTicks     = "pm.ticks"
	topicAlerts    = "pm.alerts"
	topicSnapshots = "pm.snapshots"
	topicTop       = "pm.top"
	topicMarkets   = "pm.markets"

	consumerGroup = "processor"
)

// Config is the processor's environment configuration (design-plan.md
// section 13).
type Config struct {
	KafkaBrokers     []string `env:"KAFKA_BROKERS" envDefault:"localhost:19092" envSeparator:","`
	SurgeThresholdPP float64  `env:"SURGE_THRESHOLD_PP" envDefault:"5"`
	SurgeWindowS     int      `env:"SURGE_WINDOW_S" envDefault:"60"`
	MetricsAddr      string   `env:"METRICS_ADDR" envDefault:":9090"`
}

// topInterval is how often pm.top is recomputed and produced (design-plan.md
// section 4.2: "roughly every 1 s"). Not exposed as an env var - same
// reasoning as the ingestor's refreshInterval/bufferSize constants.
const topInterval = time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "processor")

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

	store := newStateStore(
		stats.NewSurgeDetector(cfg.SurgeThresholdPP, time.Duration(cfg.SurgeWindowS)*time.Second, stats.DefaultCooldown),
		cfg.SurgeWindowS,
	)

	// pm.markets is small and compacted; give it a head start so
	// alert question/outcome are populated by the time real pm.raw
	// processing begins (best-effort: a market discovered after startup is
	// still picked up, just not necessarily before the first alert on it).
	go func() {
		if err := loadMarkets(ctx, log, cfg.KafkaBrokers, store); err != nil && ctx.Err() == nil {
			log.Error("pm.markets consumer", "error", err)
		}
	}()
	time.Sleep(500 * time.Millisecond)

	if err := bootstrapSnapshots(ctx, log, cfg.KafkaBrokers, store); err != nil {
		log.Error("bootstrap pm.snapshots", "error", err)
		os.Exit(1)
	}

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Error("kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	// At-least-once (design-plan.md 4.2): auto-commit is disabled, and
	// offsets for a poll's records are committed only once every derived
	// record produced from it has been durably acked. ConsumeResetOffset
	// AtEnd only takes effect the first time this group joins (no committed
	// offset yet); a restart resumes from the last commit.
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, consumerGroup, []string{topicRaw},
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		log.Error("kafka consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	go runTopLoop(ctx, log, producer, store)

	runConsumeLoop(ctx, log, consumer, producer, store, newPartitionLag())

	log.Info("shutting down")
}

// runConsumeLoop polls pm.raw, feeds each record through store, produces the
// derived records, and commits pm.raw offsets only after those produces are
// durably acked (design-plan.md 4.2's at-least-once note).
func runConsumeLoop(ctx context.Context, log *slog.Logger, consumer, producer *kgo.Client, store *stateStore, lag *partitionLag) {
	for {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("pm.raw fetch error", "error", err)
		}
		if fetches.Empty() {
			continue
		}

		lag.update(fetches)

		var produceErr error
		fetches.EachRecord(func(r *kgo.Record) {
			handleRecord(ctx, log, producer, store, r, &produceErr)
		})

		if err := producer.Flush(ctx); err != nil {
			produceErr = err
		}
		if produceErr != nil {
			// Do not commit: on restart this batch's pm.raw offsets are
			// replayed, which is exactly the intended at-least-once
			// behavior. Records already durably produced this round may be
			// re-produced too (harmless: ticks/snapshots/alerts all carry
			// seq/ts and are idempotent to re-send).
			log.Warn("derived produce failed, not committing pm.raw offsets", "error", produceErr)
			continue
		}
		if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
			log.Warn("commit pm.raw offsets", "error", err)
		}
	}
}

func handleRecord(ctx context.Context, log *slog.Logger, producer *kgo.Client, store *stateStore, r *kgo.Record, produceErr *error) {
	start := time.Now()
	defer func() { metrics.ProcessorHandleSeconds.Observe(time.Since(start).Seconds()) }()

	var ev model.RawEvent
	if err := json.Unmarshal(r.Value, &ev); err != nil {
		log.Warn("decode pm.raw record", "error", err)
		return
	}
	metrics.ProcessorEventsTotal.Inc()

	out := store.handleRaw(start, ev)

	if out.tick != nil {
		produceJSON(ctx, producer, topicTicks, out.tick.AssetID, out.tick, produceErr)
	}
	if out.alert != nil {
		metrics.ProcessorAlertsTotal.Inc()
		produceJSON(ctx, producer, topicAlerts, out.alert.AssetID, out.alert, produceErr)
	}
	if out.snapshot != nil {
		produceJSON(ctx, producer, topicSnapshots, out.snapshot.AssetID, out.snapshot, produceErr)
	}
}

func produceJSON(ctx context.Context, producer *kgo.Client, topic, key string, v any, produceErr *error) {
	value, err := json.Marshal(v)
	if err != nil {
		*produceErr = err
		return
	}
	kafka.Produce(ctx, producer, topic, []byte(key), value, func(err error) {
		if err != nil {
			*produceErr = err
		}
	})
}

// partitionLag tracks processor_consume_lag (design-plan.md 4.2) as a
// persistent per-partition map rather than recomputing it from scratch on
// every poll. The previous version (see docs/benchmark.md's S1 finding)
// summed only the partitions that happened to have records in *that one*
// PollFetches batch and then Set() the gauge to that partial sum - so a
// batch touching 2 of 12 partitions silently reported lag as if the other
// 10 had none, and the gauge read misleadingly close to 0 even while real
// lag was climbing into the thousands. Tracking each partition's last-known
// lag here and always reporting the sum across all of them fixes that.
type partitionLag struct {
	mu  sync.Mutex
	lag map[int32]int64
}

func newPartitionLag() *partitionLag {
	return &partitionLag{lag: make(map[int32]int64)}
}

// update records this poll's lag for every partition that had records this
// round (partitions with nothing new this round keep their last known
// value - they are not assumed caught up), then republishes the gauge as
// the sum across every partition ever seen.
func (p *partitionLag) update(fetches kgo.Fetches) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fetches.EachPartition(func(part kgo.FetchTopicPartition) {
		if len(part.Records) == 0 {
			return
		}
		last := part.Records[len(part.Records)-1]
		p.lag[part.Partition] = part.HighWatermark - (last.Offset + 1)
	})
	var total int64
	for _, l := range p.lag {
		total += l
	}
	metrics.ProcessorConsumeLag.Set(float64(total))
}

// runTopLoop recomputes and produces pm.top roughly every topInterval
// (design-plan.md section 4.2), keyed "top" on its single partition.
func runTopLoop(ctx context.Context, log *slog.Logger, producer *kgo.Client, store *stateStore) {
	t := time.NewTicker(topInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			top := model.TopList{V: model.TopVersion, TS: now.UnixMilli(), D: store.topMovers(now)}
			value, err := json.Marshal(top)
			if err != nil {
				log.Warn("marshal pm.top", "error", err)
				continue
			}
			kafka.Produce(ctx, producer, topicTop, []byte("top"), value, func(err error) {
				if err != nil {
					log.Warn("produce pm.top failed", "error", err)
				}
			})
		}
	}
}
