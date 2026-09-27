package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nawat-john/oddspulse/internal/model"
)

// idlePollTimeout/idleRounds bound how long bootstrap consumption of a
// compacted topic waits before deciding there is nothing more to read right
// now. Same idle-timeout heuristic as cmd/processor/bootstrap.go (see its
// ponytail note); kept as a small local copy rather than a shared
// internal/kafka helper, since this is the only other place it is needed.
const (
	idlePollTimeout = 500 * time.Millisecond
	idleRounds      = 3
)

// bootstrap consumes pm.snapshots and pm.markets from the beginning to
// build the gateway's initial caches (design-plan.md section 4.3: "read
// pm.snapshots and pm.markets from earliest to build a cache at startup").
func (gw *gatewayServer) bootstrap(ctx context.Context, brokers []string) error {
	if err := gw.loadSnapshots(ctx, brokers); err != nil {
		return err
	}
	return gw.loadMarkets(ctx, brokers)
}

func (gw *gatewayServer) loadSnapshots(ctx context.Context, brokers []string) error {
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
		if jerr := json.Unmarshal(r.Value, &t); jerr != nil {
			gw.log.Warn("bootstrap: decode pm.snapshots record", "error", jerr)
			return
		}
		gw.hub.SeedCache(t.AssetID, r.Value)
		n++
	})
	gw.log.Info("bootstrap: pm.snapshots loaded", "assets", n)
	return err
}

func (gw *gatewayServer) loadMarkets(ctx context.Context, brokers []string) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topicMarkets),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	n := 0
	err = consumeUntilIdle(ctx, cl, func(r *kgo.Record) {
		if r.Value == nil {
			return
		}
		var m model.Market
		if jerr := json.Unmarshal(r.Value, &m); jerr != nil {
			gw.log.Warn("bootstrap: decode pm.markets record", "error", jerr)
			return
		}
		gw.markets.set(m)
		n++
	})
	gw.log.Info("bootstrap: pm.markets loaded", "markets", n)
	return err
}

// consumeUntilIdle polls cl and calls onRecord for every fetched record,
// stopping once idleRounds consecutive polls (each bounded by
// idlePollTimeout) return no records - i.e. once the topic appears caught up.
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
