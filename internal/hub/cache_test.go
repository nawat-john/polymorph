package hub

import "testing"

// TestSetTickCopiesInput proves the cache does not retain the caller's
// slice: mutating the source buffer after caching must not affect what
// getTick later returns. This is the actual bug (design-plan.md 4.3's
// snapshot cache was pinning whole Kafka fetch buffers alive) - a passing
// test here means the fix is a real copy, not just "doesn't crash".
func TestSetTickCopiesInput(t *testing.T) {
	c := newCache()

	src := []byte(`{"a":"a1","p":0.5}`)
	c.setTick("a1", src)

	// Simulate the source buffer being reused/overwritten, as a Kafka fetch
	// buffer can be once the batch it came from is released.
	for i := range src {
		src[i] = 'X'
	}

	got, ok := c.getTick("a1")
	if !ok {
		t.Fatalf("getTick(a1) not found")
	}
	if string(got) != `{"a":"a1","p":0.5}` {
		t.Fatalf("cached tick = %s, want it unaffected by the later mutation of the source buffer", got)
	}
}
