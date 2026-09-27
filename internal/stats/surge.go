package stats

import (
	"math"
	"time"
)

// DefaultCooldown is the per-asset cooldown between surge alerts
// (design-plan.md section 4.2: "a 2-minute per-asset cooldown to avoid
// duplicate alerts"). Not exposed as an env var - the design plan gives one
// fixed value and no reason to tune it per deployment.
const DefaultCooldown = 2 * time.Minute

// SurgeDetector flags a price surge - |delta price| >= ThresholdPP percentage
// points within Window - once per asset per Cooldown period.
type SurgeDetector struct {
	ThresholdPP float64
	Window      time.Duration
	Cooldown    time.Duration

	lastFired map[string]time.Time
}

// NewSurgeDetector returns a SurgeDetector with the given threshold (in
// percentage points), window and cooldown.
func NewSurgeDetector(thresholdPP float64, window, cooldown time.Duration) *SurgeDetector {
	return &SurgeDetector{
		ThresholdPP: thresholdPP,
		Window:      window,
		Cooldown:    cooldown,
		lastFired:   make(map[string]time.Time),
	}
}

// Check reports whether assetID should fire a surge alert right now, given
// its ring buffer and current price. It returns the from/to prices and true
// only when the change over Window meets ThresholdPP and the asset is not
// still in its cooldown period; a firing check resets the cooldown.
func (s *SurgeDetector) Check(assetID string, now time.Time, ring *RingBuffer, current float64) (from, to float64, fired bool) {
	past, ok := ring.Nearest(now.Add(-s.Window))
	if !ok {
		return 0, 0, false
	}
	if math.Abs((current-past.Price)*100) < s.ThresholdPP {
		return 0, 0, false
	}
	if last, seen := s.lastFired[assetID]; seen && now.Sub(last) < s.Cooldown {
		return 0, 0, false
	}
	s.lastFired[assetID] = now
	return past.Price, current, true
}
