package main

import (
	"sort"
	"sync"
)

// latencySketch is a small fixed-capacity ring buffer of recent end-to-end
// latency samples (milliseconds), used to compute a real p99 for the "sys"
// channel (design-plan.md section 6) without querying Prometheus from
// inside the process.
//
// ponytail: nearest-rank over a copy of the buffer, O(n log n) once a
// second over at most `cap` samples - simple and correct for a once-a-second
// display; upgrade to a streaming quantile sketch (e.g. t-digest) only if
// this shows up in a profile.
type latencySketch struct {
	mu     sync.Mutex
	buf    []float64
	idx    int
	filled bool
}

func newLatencySketch(capacity int) *latencySketch {
	return &latencySketch{buf: make([]float64, capacity)}
}

func (s *latencySketch) add(ms float64) {
	s.mu.Lock()
	s.buf[s.idx] = ms
	s.idx++
	if s.idx == len(s.buf) {
		s.idx = 0
		s.filled = true
	}
	s.mu.Unlock()
}

// p99 returns the 99th-percentile latency (nearest-rank) over the samples
// currently held, or 0 if none have been recorded yet.
func (s *latencySketch) p99() float64 {
	s.mu.Lock()
	n := s.idx
	if s.filled {
		n = len(s.buf)
	}
	cp := make([]float64, n)
	copy(cp, s.buf[:n])
	s.mu.Unlock()

	if n == 0 {
		return 0
	}
	sort.Float64s(cp)
	rank := int(float64(n) * 0.99)
	if rank >= n {
		rank = n - 1
	}
	return cp[rank]
}
