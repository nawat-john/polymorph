package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nawat-john/oddspulse/internal/metrics"
	"github.com/nawat-john/oddspulse/internal/model"
	"github.com/nawat-john/oddspulse/internal/wsproto"
)

// runConsumeLoop polls pm.ticks/pm.alerts (and pm.top, added by the caller's
// topic list) and fans each record out through the hub. This never commits
// offsets explicitly: each gateway instance uses its own throwaway consumer
// group (design-plan.md 4.3's "broadcast" pattern), so committed offsets
// from a dead instance are never revisited.
func (gw *gatewayServer) runConsumeLoop(ctx context.Context, consumer *kgo.Client) {
	for {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if err := fetches.Err(); err != nil {
			gw.log.Warn("consume error", "error", err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			gw.inEvents.Add(1)
			switch r.Topic {
			case topicTicks:
				gw.handleTickRecord(r.Value)
			case topicAlerts:
				gw.hub.PublishAlert(r.Value)
			case topicTop:
				gw.handleTopRecord(r.Value)
			}
		})
	}
}

// handleTickRecord parses just enough of the record to route it and record
// e2e latency, then hands the ORIGINAL bytes to the hub - never re-marshaled
// (design-plan.md 4.3: "pre-encoding").
func (gw *gatewayServer) handleTickRecord(raw []byte) {
	var t model.Tick
	if err := json.Unmarshal(raw, &t); err != nil {
		gw.log.Warn("decode pm.ticks record", "error", err)
		return
	}
	if t.RecvTS > 0 {
		latMs := float64(time.Now().UnixMilli() - t.RecvTS)
		if latMs >= 0 {
			metrics.GatewayE2ELatencySeconds.Observe(latMs / 1000)
			gw.latency.add(latMs)
		}
	}
	gw.hub.PublishTick(t.AssetID, raw)
}

// handleTopRecord reshapes one pm.top record (model.TopList, with v/ts
// wrapper fields) into the bare `[{"a":...,"c5m":...}, ...]` array the "top"
// WS channel sends as D (design-plan.md section 6). This is the one
// unavoidable re-encode in the fan-out path: it happens once per pm.top
// record (roughly once a second), never per client.
func (gw *gatewayServer) handleTopRecord(raw []byte) {
	var top model.TopList
	if err := json.Unmarshal(raw, &top); err != nil {
		gw.log.Warn("decode pm.top record", "error", err)
		return
	}
	d, err := json.Marshal(top.D)
	if err != nil {
		gw.log.Warn("marshal pm.top entries", "error", err)
		return
	}
	gw.hub.PublishTop(d)
}

// sysInterval is how often the "sys" channel is broadcast (design-plan.md
// section 6: "sent every 1s").
const sysInterval = time.Second

// runSysLoop computes real, live sys-channel numbers - client count, ingest/
// outbound rates from the running counters, and a measured p99 e2e latency -
// and broadcasts them once a second.
func (gw *gatewayServer) runSysLoop(ctx context.Context) {
	t := time.NewTicker(sysInterval)
	defer t.Stop()

	lastIn, lastOut := int64(0), int64(0)
	lastT := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			dt := now.Sub(lastT).Seconds()
			if dt <= 0 {
				dt = sysInterval.Seconds()
			}
			curIn := gw.inEvents.Load()
			curOut := gw.hub.MessagesOut()

			stats := wsproto.SysStats{
				Clients: gw.hub.ClientCount(),
				InEPS:   float64(curIn-lastIn) / dt,
				OutMPS:  float64(curOut-lastOut) / dt,
				P99Ms:   gw.latency.p99(),
			}
			lastIn, lastOut, lastT = curIn, curOut, now

			b, err := json.Marshal(stats)
			if err != nil {
				gw.log.Warn("marshal sys stats", "error", err)
				continue
			}
			gw.hub.PublishSys(b)
		}
	}
}
