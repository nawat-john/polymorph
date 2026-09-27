package stats

import "time"

// Change windows used for AssetState.Snapshot (design-plan.md section 4.2).
const (
	Window1m = time.Minute
	Window5m = 5 * time.Minute
	Window1h = time.Hour
)

// AssetState is the processor's per-asset in-memory state, exactly as
// sketched in design-plan.md section 4.2.
type AssetState struct {
	AssetID   string
	MarketID  string
	Last      float64 // latest price (mid or last trade)
	BestBid   float64
	BestAsk   float64
	Open24h   float64
	Ring      *RingBuffer // price at 1 point per second, last 15 minutes
	UpdatedAt time.Time
}

// NewAssetState returns a fresh AssetState for assetID/marketID with an
// empty ring buffer.
func NewAssetState(assetID, marketID string) *AssetState {
	return &AssetState{
		AssetID:  assetID,
		MarketID: marketID,
		Ring:     NewRingBuffer(),
	}
}

// Update records a new observed price at time now: it always updates Last
// and UpdatedAt, and feeds Ring (which throttles itself to one sample per
// second).
func (a *AssetState) Update(now time.Time, price float64) {
	a.Last = price
	a.UpdatedAt = now
	a.Ring.Add(now, price)
}

// Changes are the derived statistics computed from an AssetState's ring
// buffer (design-plan.md section 4.2). A false ok* means not enough history
// yet - the field is left at its zero value.
//
// Note: Window1h (15 min < 1h) can never be satisfied by a 15-minute Ring,
// so Chg1h/OK1h is effectively always unavailable at the current Lookback -
// this mirrors an inconsistency in design-plan.md itself (a 15-minute ring
// alongside a 1-hour change window). Kept for API completeness/section 4.2
// fidelity; not wired to any Kafka schema today (model.Tick only carries
// c1m/c5m). A future phase would need to either extend Lookback or drop
// Chg1h.
type Changes struct {
	Chg1m float64
	OK1m  bool
	Chg5m float64
	OK5m  bool
	Chg1h float64
	OK1h  bool
	Vol5m float64
	OKVol bool
}

// Snapshot computes chg_1m/chg_5m/chg_1h (percentage points) and vol_5m
// (stddev of 5-minute log-returns) from the asset's current Ring and Last
// price, as of now.
func (a *AssetState) Snapshot(now time.Time) Changes {
	var c Changes
	if p, ok := a.Ring.Nearest(now.Add(-Window1m)); ok {
		c.Chg1m = (a.Last - p.Price) * 100
		c.OK1m = true
	}
	if p, ok := a.Ring.Nearest(now.Add(-Window5m)); ok {
		c.Chg5m = (a.Last - p.Price) * 100
		c.OK5m = true
	}
	if p, ok := a.Ring.Nearest(now.Add(-Window1h)); ok {
		c.Chg1h = (a.Last - p.Price) * 100
		c.OK1h = true
	}
	if v, ok := a.Ring.Volatility(now, Window5m); ok {
		c.Vol5m = v
		c.OKVol = true
	}
	return c
}
