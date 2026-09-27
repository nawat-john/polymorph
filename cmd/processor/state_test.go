package main

import (
	"testing"
	"time"

	"github.com/nawat-john/oddspulse/internal/model"
	"github.com/nawat-john/oddspulse/internal/stats"
)

func newTestStore() *stateStore {
	return newStateStore(stats.NewSurgeDetector(5, 60*time.Second, 2*time.Minute), 60)
}

func TestHandleRawDedupesUnchangedPrice(t *testing.T) {
	s := newTestStore()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	ev := model.RawEvent{Kind: model.KindQuote, AssetID: "a1", MarketID: "m1", Price: 0.40, RecvTS: 1}

	out := s.handleRaw(base, ev)
	if out.tick == nil {
		t.Fatalf("first event with a real price: tick = nil, want non-nil")
	}
	if out.snapshot == nil {
		t.Fatalf("first event: snapshot = nil, want non-nil (first snapshot ever produced)")
	}

	// Same price again a moment later: no tick (deduped), and no snapshot
	// yet either since less than a second has passed.
	out = s.handleRaw(base.Add(100*time.Millisecond), ev)
	if out.tick != nil {
		t.Errorf("unchanged-price event produced a tick, want nil (dedupe)")
	}
	if out.snapshot != nil {
		t.Errorf("snapshot produced within the 1s throttle window, want nil")
	}

	// Still the same price a full second later: still no tick, but a
	// snapshot is due again.
	out = s.handleRaw(base.Add(1100*time.Millisecond), ev)
	if out.tick != nil {
		t.Errorf("unchanged-price event produced a tick, want nil (dedupe)")
	}
	if out.snapshot == nil {
		t.Errorf("snapshot = nil after the 1s throttle elapsed, want non-nil")
	}

	// Price actually changes: a new tick.
	ev2 := ev
	ev2.Price = 0.41
	out = s.handleRaw(base.Add(1200*time.Millisecond), ev2)
	if out.tick == nil {
		t.Fatalf("changed-price event: tick = nil, want non-nil")
	}
	if out.tick.Price != 0.41 {
		t.Errorf("tick.Price = %v, want 0.41", out.tick.Price)
	}
	if out.tick.Seq != 2 {
		t.Errorf("tick.Seq = %d, want 2 (second real price change)", out.tick.Seq)
	}
}

func TestHandleRawIgnoresTickSizeChange(t *testing.T) {
	s := newTestStore()
	ev := model.RawEvent{Kind: model.KindTickSizeChange, AssetID: "a1", MarketID: "m1"}
	out := s.handleRaw(time.Now(), ev)
	if out.tick != nil || out.alert != nil || out.snapshot != nil {
		t.Errorf("tick_size_change (no price) produced output: %+v, want all nil", out)
	}
}

func TestHandleRawFiresAlertWithMarketMetadata(t *testing.T) {
	s := newTestStore()
	s.updateMarket(model.Market{
		MarketID:     "m1",
		Question:     "Will X happen?",
		Outcomes:     []string{"Yes", "No"},
		ClobTokenIDs: []string{"a1", "a2"},
	})

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i <= 60; i++ {
		s.handleRaw(base.Add(time.Duration(i)*time.Second), model.RawEvent{
			Kind: model.KindQuote, AssetID: "a1", MarketID: "m1", Price: 0.40,
		})
	}

	out := s.handleRaw(base.Add(60*time.Second), model.RawEvent{
		Kind: model.KindQuote, AssetID: "a1", MarketID: "m1", Price: 0.55,
	})
	if out.alert == nil {
		t.Fatalf("alert = nil, want non-nil on a 15pp move within 60s")
	}
	if out.alert.Question != "Will X happen?" || out.alert.Outcome != "Yes" {
		t.Errorf("alert question/outcome = %q/%q, want %q/%q",
			out.alert.Question, out.alert.Outcome, "Will X happen?", "Yes")
	}
	if out.alert.From != 0.40 || out.alert.To != 0.55 {
		t.Errorf("alert from/to = %v/%v, want 0.40/0.55", out.alert.From, out.alert.To)
	}
}

func TestRestoreSnapshotSeedsState(t *testing.T) {
	s := newTestStore()
	s.restoreSnapshot(model.Tick{AssetID: "a1", MarketID: "m1", Price: 0.6, Bid: 0.59, Ask: 0.61, Seq: 7, ProcTS: 1000})

	e, ok := s.assets["a1"]
	if !ok {
		t.Fatalf("asset a1 not present after restoreSnapshot")
	}
	if e.state.Last != 0.6 || e.seq != 7 {
		t.Errorf("restored state = {Last:%v seq:%v}, want {0.6 7}", e.state.Last, e.seq)
	}
	if e.state.Ring.Len() != 0 {
		t.Errorf("restored ring has %d samples, want 0 (design plan: ring starts fresh)", e.state.Ring.Len())
	}
}
