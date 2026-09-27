package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nawat-john/oddspulse/internal/kafka"
	"github.com/nawat-john/oddspulse/internal/model"
)

// bootstrapSnapshots consumes pm.snapshots from the beginning and restores
// store's in-memory state from it (design-plan.md section 4.2: "Bootstrap on
// startup: consume pm.snapshots from earliest to rebuild in-memory state").
// It returns once every partition is read up to its end offset at the start
// of the call (kafka.ConsumeUpToEnd).
func bootstrapSnapshots(ctx context.Context, log *slog.Logger, brokers []string, store *stateStore) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topicSnapshots),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	n := 0
	err = kafka.ConsumeUpToEnd(ctx, cl, topicSnapshots, func(r *kgo.Record) {
		if r.Value == nil {
			return // compaction tombstone
		}
		var t model.Tick
		if err := json.Unmarshal(r.Value, &t); err != nil {
			log.Warn("bootstrap: decode pm.snapshots record", "error", err)
			return
		}
		store.restoreSnapshot(t)
		n++
	})
	log.Info("bootstrap: pm.snapshots loaded", "assets", n)
	return err
}

// loadMarkets consumes pm.markets, keeping store's market metadata (used for
// Alert.Question/Outcome) up to date for as long as ctx is alive. It never
// returns until ctx is done (or the client errors), so callers that need it
// merely "warmed up" first should run it in a goroutine and give it a brief
// head start rather than waiting on it.
func loadMarkets(ctx context.Context, log *slog.Logger, brokers []string, store *stateStore) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topicMarkets),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil {
			log.Warn("pm.markets consume error", "error", err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Value == nil {
				return
			}
			var m model.Market
			if err := json.Unmarshal(r.Value, &m); err != nil {
				log.Warn("decode pm.markets record", "error", err)
				return
			}
			store.updateMarket(m)
		})
	}
}
