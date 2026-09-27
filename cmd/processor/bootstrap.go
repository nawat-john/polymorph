package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nawat-john/oddspulse/internal/model"
)

// idlePollTimeout/idleRounds bound how long bootstrapSnapshots/loadMarkets
// wait for a compacted topic to "catch up" before deciding there is nothing
// more to read right now.
//
// ponytail: this is an idle-timeout heuristic, not an exact end-offset check
// (which would need github.com/twmb/franz-go/pkg/kadm - a dependency not
// already in go.mod). pm.markets/pm.snapshots are small compacted topics
// (one row per market/asset, capped by PM_MAX_ASSETS), so a couple of
// consecutive empty polls reliably means "caught up" in practice. Upgrade to
// a real end-offset check (kadm.ListEndOffsets) if this ever proves flaky.
const (
	idlePollTimeout = 500 * time.Millisecond
	idleRounds      = 3
)

// bootstrapSnapshots consumes pm.snapshots from the beginning and restores
// store's in-memory state from it (design-plan.md section 4.2: "Bootstrap on
// startup: consume pm.snapshots from earliest to rebuild in-memory state").
// It returns once the topic appears caught up.
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
	err = consumeUntilIdle(ctx, cl, func(r *kgo.Record) {
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
// head start rather than waiting on it, the same way its bootstrap-style
// siblings do for a compacted topic.
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

// consumeUntilIdle polls cl and calls onRecord for every fetched record,
// stopping once idleRounds consecutive polls (each bounded by
// idlePollTimeout) return no records.
func consumeUntilIdle(ctx context.Context, cl *kgo.Client, onRecord func(*kgo.Record)) error {
	idle := 0
	for idle < idleRounds {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		pctx, cancel := context.WithTimeout(ctx, idlePollTimeout)
		fetches := cl.PollFetches(pctx)
		cancel()

		empty := true
		fetches.EachRecord(func(r *kgo.Record) {
			empty = false
			onRecord(r)
		})
		if empty {
			idle++
		} else {
			idle = 0
		}
	}
	return nil
}
