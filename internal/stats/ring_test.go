package stats

import (
	"math"
	"testing"
	"time"
)

func TestRingBufferWraparound(t *testing.T) {
	r := NewRingBuffer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Fill past capacity: ringSize (900) + 50 one-second samples. Price
	// equals the sample's 0-based sequence number, so the retained window
	// can be checked precisely.
	total := ringSize + 50
	for i := 0; i < total; i++ {
		r.Add(base.Add(time.Duration(i)*time.Second), float64(i))
	}

	if got := r.Len(); got != ringSize {
		t.Fatalf("Len() = %d, want %d (buffer should be full, not overflowed)", got, ringSize)
	}

	pts := r.Points()
	if len(pts) != ringSize {
		t.Fatalf("Points() len = %d, want %d", len(pts), ringSize)
	}

	// The oldest retained sample is the one 50 seconds after the very first
	// (the first 50 must have been overwritten), and samples must be in
	// ascending time/price order.
	wantOldestPrice := float64(50)
	if pts[0].Price != wantOldestPrice {
		t.Errorf("oldest retained price = %v, want %v", pts[0].Price, wantOldestPrice)
	}
	wantNewestPrice := float64(total - 1)
	if last := pts[len(pts)-1].Price; last != wantNewestPrice {
		t.Errorf("newest retained price = %v, want %v", last, wantNewestPrice)
	}
	for i := 1; i < len(pts); i++ {
		if !pts[i].TS.After(pts[i-1].TS) {
			t.Fatalf("Points() not strictly ascending at index %d: %v then %v", i, pts[i-1].TS, pts[i].TS)
		}
		if pts[i].Price != pts[i-1].Price+1 {
			t.Fatalf("Points() price gap at index %d: %v then %v", i, pts[i-1].Price, pts[i].Price)
		}
	}
}

func TestRingBufferAddThrottlesToOnePerSecond(t *testing.T) {
	r := NewRingBuffer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	r.Add(base, 1)
	r.Add(base.Add(200*time.Millisecond), 2) // too soon, should be dropped
	r.Add(base.Add(999*time.Millisecond), 3) // still too soon
	r.Add(base.Add(1*time.Second), 4)        // exactly one interval later, kept

	if got := r.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2 (only the first and the >=1s-later sample)", got)
	}
	pts := r.Points()
	if pts[0].Price != 1 || pts[1].Price != 4 {
		t.Fatalf("Points() = %+v, want prices [1, 4]", pts)
	}
}

func TestRingBufferAddIgnoresOutOfOrder(t *testing.T) {
	r := NewRingBuffer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	r.Add(base.Add(10*time.Second), 10)
	r.Add(base, 0) // earlier than the last sample: ignored

	if got := r.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1 (out-of-order Add must be ignored)", got)
	}
}

func TestRingBufferNearest(t *testing.T) {
	r := NewRingBuffer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		r.Add(base.Add(time.Duration(i)*time.Second), float64(i))
	}

	// Exactly on a sample.
	p, ok := r.Nearest(base.Add(5 * time.Second))
	if !ok || p.Price != 5 {
		t.Fatalf("Nearest(t+5s) = (%v, %v), want (5, true)", p, ok)
	}

	// Between samples: nearest at-or-before.
	p, ok = r.Nearest(base.Add(5*time.Second + 400*time.Millisecond))
	if !ok || p.Price != 5 {
		t.Fatalf("Nearest(t+5.4s) = (%v, %v), want (5, true)", p, ok)
	}

	// Before every sample: not found.
	_, ok = r.Nearest(base.Add(-time.Second))
	if ok {
		t.Fatalf("Nearest(before first sample) ok = true, want false")
	}
}

func TestRingBufferVolatilityHandComputed(t *testing.T) {
	r := NewRingBuffer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	prices := []float64{0.50, 0.50, 0.60, 0.60, 0.50}
	for i, p := range prices {
		r.Add(base.Add(time.Duration(i)*time.Second), p)
	}

	// Hand-computed (see log-return derivation): returns are
	// [0, ln(1.2), 0, ln(1/1.2)], sample stddev (n-1) of those is
	// ~0.1488649278.
	got, ok := r.Volatility(base.Add(4*time.Second), 10*time.Second)
	if !ok {
		t.Fatalf("Volatility() ok = false, want true")
	}
	want := 0.1488649278
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("Volatility() = %.10f, want %.10f", got, want)
	}
}

func TestRingBufferVolatilityInsufficientData(t *testing.T) {
	r := NewRingBuffer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.Add(base, 0.5)
	r.Add(base.Add(time.Second), 0.5)

	if _, ok := r.Volatility(base.Add(time.Second), 10*time.Second); ok {
		t.Fatalf("Volatility() with only 2 samples ok = true, want false")
	}
}
