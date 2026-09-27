package stats

import (
	"math"
	"sort"
)

// Mover is one asset's 5-minute change, ranked by TopMovers.
type Mover struct {
	AssetID string
	Chg5m   float64
}

// TopMovers returns (a copy of) the n movers with the largest |Chg5m|,
// largest first (design-plan.md section 4.2: "sort by |chg_5m|, top 50").
// n <= 0 or n >= len(movers) returns every mover sorted.
func TopMovers(movers []Mover, n int) []Mover {
	sorted := make([]Mover, len(movers))
	copy(sorted, movers)
	sort.Slice(sorted, func(i, j int) bool {
		return math.Abs(sorted[i].Chg5m) > math.Abs(sorted[j].Chg5m)
	})
	if n > 0 && n < len(sorted) {
		sorted = sorted[:n]
	}
	return sorted
}
