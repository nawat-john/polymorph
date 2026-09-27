// Package stats is the processor's pure-logic core (design-plan.md section
// 4.2): a per-asset ring buffer of recent prices, change/volatility
// calculations over it, a surge detector, and top-movers ranking. It has no
// Kafka dependency so it stays fully unit-testable; cmd/processor wires it
// to pm.raw/pm.ticks/pm.alerts/pm.snapshots/pm.top.
package stats

import (
	"math"
	"time"
)

// SampleInterval is the ring buffer's sampling rate: at most one price point
// is retained per asset per second (design-plan.md section 4.2).
const SampleInterval = time.Second

// Lookback is how far back the ring buffer retains samples (design-plan.md
// section 4.2: "last 15 minutes").
const Lookback = 15 * time.Minute

// ringSize is the number of slots needed to hold Lookback at SampleInterval.
const ringSize = int(Lookback / SampleInterval) // 900

// Point is one sample in a RingBuffer.
type Point struct {
	TS    time.Time
	Price float64
}

// RingBuffer is a fixed-size circular buffer of Points, sampled at most once
// per SampleInterval, covering the trailing Lookback window. Zero value is
// not ready for use; call NewRingBuffer.
type RingBuffer struct {
	buf   [ringSize]Point
	next  int // index the next Add will write to
	count int // number of valid entries (caps at ringSize)
}

// NewRingBuffer returns an empty RingBuffer.
func NewRingBuffer() *RingBuffer {
	return &RingBuffer{}
}

// Add records a price sample at ts, unless less than SampleInterval has
// elapsed since the last recorded sample (design-plan.md's "1 point per
// second" - this throttles the buffer independently of how often the caller
// observes new prices). Samples must be added in non-decreasing ts order;
// out-of-order calls (e.g. a replayed/reordered event) are ignored.
func (r *RingBuffer) Add(ts time.Time, price float64) {
	if r.count > 0 {
		last := r.buf[r.lastIndex()]
		if ts.Before(last.TS) || ts.Sub(last.TS) < SampleInterval {
			return
		}
	}
	r.buf[r.next] = Point{TS: ts, Price: price}
	r.next = (r.next + 1) % ringSize
	if r.count < ringSize {
		r.count++
	}
}

// Len returns the number of samples currently retained.
func (r *RingBuffer) Len() int { return r.count }

func (r *RingBuffer) lastIndex() int {
	return (r.next - 1 + ringSize) % ringSize
}

// oldestIndex returns the buffer index of the oldest retained sample.
func (r *RingBuffer) oldestIndex() int {
	if r.count < ringSize {
		return 0
	}
	return r.next
}

// ascending calls fn for every retained sample, oldest first.
func (r *RingBuffer) ascending(fn func(Point)) {
	start := r.oldestIndex()
	for i := 0; i < r.count; i++ {
		fn(r.buf[(start+i)%ringSize])
	}
}

// Points returns a copy of every retained sample, oldest first. Mainly for
// tests; hot paths should prefer Nearest/Volatility, which avoid the
// allocation.
func (r *RingBuffer) Points() []Point {
	out := make([]Point, 0, r.count)
	r.ascending(func(p Point) { out = append(out, p) })
	return out
}

// Nearest returns the retained sample with the largest timestamp at or
// before target, and true - or the zero Point and false if every retained
// sample is after target (including when the buffer is empty).
//
// ponytail: this is an O(ringSize) scan (<=900 samples), not a binary
// search. ringSize is small and fixed by design, so the simple scan is fine;
// switch to a binary search over the ascending order if ringSize ever grows
// enough for it to matter.
func (r *RingBuffer) Nearest(target time.Time) (Point, bool) {
	var best Point
	found := false
	r.ascending(func(p Point) {
		if !p.TS.After(target) {
			best = p
			found = true
		}
	})
	return best, found
}

// Volatility returns the sample standard deviation of the 1-sample log
// returns within the trailing window (design-plan.md section 4.2: "vol_5m =
// standard deviation of the 5-minute log-return"), and true - or (0, false)
// if fewer than two returns (three samples) fall in the window.
func (r *RingBuffer) Volatility(now time.Time, window time.Duration) (float64, bool) {
	cutoff := now.Add(-window)
	var pts []Point
	r.ascending(func(p Point) {
		if !p.TS.Before(cutoff) {
			pts = append(pts, p)
		}
	})
	if len(pts) < 3 {
		return 0, false
	}

	returns := make([]float64, 0, len(pts)-1)
	for i := 1; i < len(pts); i++ {
		p0, p1 := pts[i-1].Price, pts[i].Price
		if p0 <= 0 || p1 <= 0 {
			continue // log-return undefined for non-positive prices
		}
		returns = append(returns, math.Log(p1/p0))
	}
	if len(returns) < 2 {
		return 0, false
	}
	return stddev(returns), true
}

// stddev is the sample standard deviation (n-1 denominator).
func stddev(xs []float64) float64 {
	var mean float64
	for _, x := range xs {
		mean += x
	}
	mean /= float64(len(xs))

	var sumSq float64
	for _, x := range xs {
		d := x - mean
		sumSq += d * d
	}
	return math.Sqrt(sumSq / float64(len(xs)-1))
}
