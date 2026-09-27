package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nawat-john/oddspulse/internal/model"
)

// pollTimeout bounds each individual PollFetches call while catching up to
// the end offsets captured at bootstrap start.
const pollTimeout = 500 * time.Millisecond

// bootstrap consumes pm.snapshots and pm.markets up to their high watermark
// at the moment bootstrap starts, to build the gateway's initial caches
// (design-plan.md section 4.3: "read pm.snapshots and pm.markets from
// earliest to build a cache at startup").
//
// This bounds bootstrap by an explicit end-offset snapshot (kadm.
// ListEndOffsets) rather than an "N consecutive idle polls" heuristic.
// Verified live against the docker-compose stack: pm.snapshots is a
// continuously-written compacted topic (the processor produces a fresh
// snapshot roughly once a second per active asset), and once the topic
// accumulates more than a few minutes of not-yet-compacted history, an
// idle-based heuristic can take on the order of two minutes to notice 3
// consecutive quiet polls - it keeps getting reset by the live trickle
// it is racing against. Stopping at a fixed target offset instead makes
// bootstrap duration proportional to the topic's *current* size, not to
// how continuously it happens to be receiving new records right now.
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
	err = consumeUpToEnd(ctx, cl, topicSnapshots, func(r *kgo.Record) {
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

// loadMarkets catches up on pm.markets like loadSnapshots, then keeps
// consuming it on the same client for the gateway's lifetime (gw.appCtx).
// Bootstrap alone raced the ingestor's first market discovery on a fresh
// stack: gateway and ingestor start together, so the catch-up usually saw an
// empty topic and the Market Wall stayed empty until a gateway restart.
func (gw *gatewayServer) loadMarkets(ctx context.Context, brokers []string) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topicMarkets),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}

	n := 0
	err = consumeUpToEnd(ctx, cl, topicMarkets, func(r *kgo.Record) {
		if gw.applyMarket(r) {
			n++
		}
	})
	gw.log.Info("bootstrap: pm.markets loaded", "markets", n)
	if err != nil {
		cl.Close()
		return err
	}

	go func() {
		defer cl.Close()
		for {
			fetches := cl.PollFetches(gw.appCtx)
			if gw.appCtx.Err() != nil {
				return
			}
			if ferr := fetches.Err(); ferr != nil {
				gw.log.Warn("pm.markets consume error", "error", ferr)
			}
			fetches.EachRecord(func(r *kgo.Record) { gw.applyMarket(r) })
		}
	}()
	return nil
}

// applyMarket decodes one pm.markets record into gw.markets, reporting
// whether it stored anything (tombstones and bad records are skipped).
func (gw *gatewayServer) applyMarket(r *kgo.Record) bool {
	if r.Value == nil {
		return false
	}
	var m model.Market
	if err := json.Unmarshal(r.Value, &m); err != nil {
		gw.log.Warn("decode pm.markets record", "error", err)
		return false
	}
	gw.markets.set(m)
	return true
}

// consumeUpToEnd polls cl (already subscribed to topic via ConsumeTopics)
// and calls onRecord for every fetched record, stopping once every
// partition has been consumed up to the high watermark recorded at the
// start of the call (a partition with no records at all is skipped
// immediately). cl must not be shared with any other goroutine. cl stays
// open afterwards (the caller owns it), so no adm.Close() here: that would
// close cl too.
func consumeUpToEnd(ctx context.Context, cl *kgo.Client, topic string, onRecord func(*kgo.Record)) error {
	adm := kadm.NewClient(cl)

	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("list end offsets for %s: %w", topic, err)
	}

	target := make(map[int32]int64)
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && o.Offset > 0 {
			target[o.Partition] = o.Offset // high watermark: next offset to be written
		}
	})

	reached := make(map[int32]bool, len(target))
	for len(reached) < len(target) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		pctx, cancel := context.WithTimeout(ctx, pollTimeout)
		fetches := cl.PollFetches(pctx)
		cancel()

		fetches.EachRecord(func(r *kgo.Record) {
			onRecord(r)
			if want, ok := target[r.Partition]; ok && r.Offset+1 >= want {
				reached[r.Partition] = true
			}
		})
	}
	return nil
}
