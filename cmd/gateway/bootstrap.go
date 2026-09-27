package main

import (
	"context"
	"encoding/json"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nawat-john/oddspulse/internal/kafka"
	"github.com/nawat-john/oddspulse/internal/model"
)

// bootstrap consumes pm.snapshots and pm.markets up to their high watermark
// at the moment bootstrap starts, to build the gateway's initial caches
// (design-plan.md section 4.3: "read pm.snapshots and pm.markets from
// earliest to build a cache at startup"). See kafka.ConsumeUpToEnd for why
// it stops at an end-offset snapshot rather than after N idle polls.
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
	err = kafka.ConsumeUpToEnd(ctx, cl, topicSnapshots, func(r *kgo.Record) {
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
	err = kafka.ConsumeUpToEnd(ctx, cl, topicMarkets, func(r *kgo.Record) {
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
