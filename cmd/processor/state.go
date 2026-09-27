package main

import (
	"sync"
	"time"

	"github.com/nawat-john/oddspulse/internal/model"
	"github.com/nawat-john/oddspulse/internal/stats"
)

// topN is the top-movers list size (design-plan.md section 4.2: "top 50").
const topN = 50

// snapshotThrottle bounds pm.snapshots production to at most once per second
// per asset (design-plan.md section 4.2).
const snapshotThrottle = time.Second

// assetEntry is one asset's processor-side bookkeeping: the pure stats.AssetState
// plus the bits that only make sense alongside Kafka production (a
// per-asset sequence number for Tick.Seq, and the last time a snapshot was
// produced for throttling).
type assetEntry struct {
	state      *stats.AssetState
	seq        int64
	lastSnapAt time.Time
}

// marketInfo is the subset of pm.markets a processor needs to fill in
// Alert.Question/Alert.Outcome (design-plan.md section 5's Alert schema);
// RawEvent already carries market_id directly, so nothing else is needed
// from pm.markets for pm.ticks/pm.snapshots.
type marketInfo struct {
	question     string
	outcomes     []string
	clobTokenIDs []string
}

// outcomeFor returns the outcome label (e.g. "Yes"/"No") for assetID within
// this market, or "" if unknown.
func (m marketInfo) outcomeFor(assetID string) string {
	for i, id := range m.clobTokenIDs {
		if id == assetID && i < len(m.outcomes) {
			return m.outcomes[i]
		}
	}
	return ""
}

// stateStore is the processor's in-memory state: per-asset AssetState plus
// the market metadata needed for alerts. Design-plan.md section 4.2: state
// naturally partitions by key=asset_id, so a sharded RWMutex is unneeded
// overkill here - one mutex is enough for a single processor instance, and
// correctness (not lock granularity) is what phase 2 needs to prove.
type stateStore struct {
	mu      sync.Mutex
	assets  map[string]*assetEntry
	markets map[string]marketInfo // market_id -> info

	surge        *stats.SurgeDetector
	surgeWindowS int
}

func newStateStore(surge *stats.SurgeDetector, surgeWindowS int) *stateStore {
	return &stateStore{
		assets:       make(map[string]*assetEntry),
		markets:      make(map[string]marketInfo),
		surge:        surge,
		surgeWindowS: surgeWindowS,
	}
}

// restoreSnapshot seeds in-memory state from a pm.snapshots record read at
// bootstrap (design-plan.md section 4.2). The ring buffer intentionally
// starts empty - "acceptable for a demo" per the design plan's own scaling
// note - so change/volatility become available again only after fresh
// history accumulates.
func (s *stateStore) restoreSnapshot(t model.Tick) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.assets[t.AssetID]
	if !ok {
		e = &assetEntry{state: stats.NewAssetState(t.AssetID, t.MarketID)}
		s.assets[t.AssetID] = e
	}
	e.state.MarketID = t.MarketID
	e.state.Last = t.Price
	e.state.BestBid = t.Bid
	e.state.BestAsk = t.Ask
	e.state.UpdatedAt = time.UnixMilli(t.ProcTS)
	if t.Seq > e.seq {
		e.seq = t.Seq
	}
}

// updateMarket records/refreshes one market's metadata from a pm.markets
// record.
func (s *stateStore) updateMarket(m model.Market) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markets[m.MarketID] = marketInfo{
		question:     m.Question,
		outcomes:     m.Outcomes,
		clobTokenIDs: m.ClobTokenIDs,
	}
}

// processed holds what handleRaw decided to produce for one pm.raw record.
type processed struct {
	tick     *model.Tick
	alert    *model.Alert
	snapshot *model.Tick
}

// handleRaw folds one RawEvent into asset state and returns what should be
// produced (design-plan.md section 4.2):
//   - tick: only on an event where price actually changed (dedupes unchanged
//     prices)
//   - alert: only when the surge detector fires
//   - snapshot: at most once per second per asset, regardless of whether the
//     price changed on this particular event
//
// A tick_size_change event (or any event with price == 0, e.g. before an
// asset's first real quote/trade) carries no usable price and is a no-op.
func (s *stateStore) handleRaw(now time.Time, ev model.RawEvent) processed {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.assets[ev.AssetID]
	if !ok {
		e = &assetEntry{state: stats.NewAssetState(ev.AssetID, ev.MarketID)}
		s.assets[ev.AssetID] = e
	}
	if ev.MarketID != "" {
		e.state.MarketID = ev.MarketID
	}
	if ev.BestBid != 0 {
		e.state.BestBid = ev.BestBid
	}
	if ev.BestAsk != 0 {
		e.state.BestAsk = ev.BestAsk
	}

	priceChanged := ev.Price != 0 && ev.Price != e.state.Last
	if ev.Price != 0 {
		e.state.Update(now, ev.Price)
	}

	var out processed
	if e.state.Last == 0 {
		return out // no real price observed for this asset yet
	}

	if priceChanged {
		e.seq++
		out.tick = s.buildTick(e, ev.RecvTS, now)

		if from, to, fired := s.surge.Check(ev.AssetID, now, e.state.Ring, e.state.Last); fired {
			mkt := s.markets[e.state.MarketID]
			out.alert = &model.Alert{
				V:        model.AlertVersion,
				AssetID:  ev.AssetID,
				MarketID: e.state.MarketID,
				Question: mkt.question,
				Outcome:  mkt.outcomeFor(ev.AssetID),
				From:     from,
				To:       to,
				WindowS:  s.surgeWindowS,
				TS:       now.UnixMilli(),
			}
		}
	}

	if now.Sub(e.lastSnapAt) >= snapshotThrottle {
		e.lastSnapAt = now
		out.snapshot = s.buildTick(e, ev.RecvTS, now)
	}
	return out
}

// buildTick renders one asset's current state as a model.Tick. Caller must
// hold s.mu.
func (s *stateStore) buildTick(e *assetEntry, recvTS int64, now time.Time) *model.Tick {
	c := e.state.Snapshot(now)
	return &model.Tick{
		V:        model.TickVersion,
		AssetID:  e.state.AssetID,
		MarketID: e.state.MarketID,
		Price:    e.state.Last,
		Bid:      e.state.BestBid,
		Ask:      e.state.BestAsk,
		Chg1m:    c.Chg1m,
		Chg5m:    c.Chg5m,
		Vol5m:    c.Vol5m,
		Seq:      e.seq,
		RecvTS:   recvTS,
		ProcTS:   now.UnixMilli(),
	}
}

// topMovers recomputes the top-movers list from every asset's current
// chg_5m (design-plan.md section 4.2: "sort by |chg_5m|, top 50").
func (s *stateStore) topMovers(now time.Time) []model.TopEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	movers := make([]stats.Mover, 0, len(s.assets))
	for id, e := range s.assets {
		if e.state.Last == 0 {
			continue
		}
		if p, ok := e.state.Ring.Nearest(now.Add(-stats.Window5m)); ok {
			movers = append(movers, stats.Mover{AssetID: id, Chg5m: (e.state.Last - p.Price) * 100})
		}
	}

	top := stats.TopMovers(movers, topN)
	out := make([]model.TopEntry, len(top))
	for i, m := range top {
		out[i] = model.TopEntry{AssetID: m.AssetID, Chg5m: m.Chg5m}
	}
	return out
}
