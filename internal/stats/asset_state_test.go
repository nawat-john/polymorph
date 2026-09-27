package stats

import (
	"math"
	"testing"
	"time"
)

func TestAssetStateSnapshotChangeCalc(t *testing.T) {
	a := NewAssetState("asset-1", "market-1")
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Known price sequence: flat at 0.40 for the first minute, then a step
	// up to 0.50 at t=60s, held through t=5m (300s).
	for i := 0; i <= 60; i++ {
		a.Update(base.Add(time.Duration(i)*time.Second), 0.40)
	}
	for i := 61; i <= 300; i++ {
		a.Update(base.Add(time.Duration(i)*time.Second), 0.50)
	}

	now := base.Add(300 * time.Second)
	c := a.Snapshot(now)

	// chg_1m compares against the sample ~60s ago (price 0.40 at t=240s is
	// still 0.50 in this sequence - the step happened at t=61s, long before
	// t=240s) -> no change expected over the last minute.
	if !c.OK1m {
		t.Fatalf("OK1m = false, want true")
	}
	if math.Abs(c.Chg1m-0) > 1e-9 {
		t.Errorf("Chg1m = %v, want ~0 (price flat at 0.50 over the last minute)", c.Chg1m)
	}

	// chg_5m looks back to ~t=0s (price 0.40); current is 0.50 ->
	// +10 percentage points.
	if !c.OK5m {
		t.Fatalf("OK5m = false, want true")
	}
	if math.Abs(c.Chg5m-10) > 1e-9 {
		t.Errorf("Chg5m = %v, want 10", c.Chg5m)
	}

	// Ring only retains 15 minutes and Window1h is 1 hour: never satisfiable
	// yet (see the Changes doc comment on this known design-plan gap).
	if c.OK1h {
		t.Errorf("OK1h = true, want false (1h lookback exceeds the 15-minute ring)")
	}
}

func TestAssetStateSnapshotNoHistoryYet(t *testing.T) {
	a := NewAssetState("asset-1", "market-1")
	now := time.Now()
	a.Update(now, 0.5)

	c := a.Snapshot(now)
	if c.OK1m || c.OK5m || c.OK1h || c.OKVol {
		t.Errorf("Snapshot() on the very first sample = %+v, want every OK* false", c)
	}
}
