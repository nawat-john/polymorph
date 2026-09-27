package stats

import (
	"testing"
	"time"
)

func TestSurgeDetectorFiresOnceThenCooldown(t *testing.T) {
	ring := NewRingBuffer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Flat at 0.40 for the first 60s, matching design-plan.md's own example
	// (>=5pp/60s threshold).
	for i := 0; i <= 60; i++ {
		ring.Add(base.Add(time.Duration(i)*time.Second), 0.40)
	}

	d := NewSurgeDetector(5, 60*time.Second, 2*time.Minute)

	// Below threshold: a 3pp move must not fire.
	if _, _, fired := d.Check("a1", base.Add(60*time.Second), ring, 0.43); fired {
		t.Fatalf("Check() fired on a 3pp move, want no fire (threshold is 5pp)")
	}

	// A 15pp jump at t=60s (0.40 -> 0.55) must fire.
	from, to, fired := d.Check("a1", base.Add(60*time.Second), ring, 0.55)
	if !fired {
		t.Fatalf("Check() did not fire on a 15pp move within the window")
	}
	if from != 0.40 || to != 0.55 {
		t.Errorf("Check() from/to = %v/%v, want 0.40/0.55", from, to)
	}

	// Immediately after: still surging, but within cooldown -> must not
	// fire again.
	if _, _, fired := d.Check("a1", base.Add(61*time.Second), ring, 0.55); fired {
		t.Fatalf("Check() fired again inside the 2-minute cooldown")
	}

	// A different asset is unaffected by asset "a1"'s cooldown.
	ring2 := NewRingBuffer()
	for i := 0; i <= 60; i++ {
		ring2.Add(base.Add(time.Duration(i)*time.Second), 0.40)
	}
	if _, _, fired := d.Check("a2", base.Add(60*time.Second), ring2, 0.55); !fired {
		t.Fatalf("Check() for a different asset did not fire; cooldown must be per-asset")
	}

	// After the cooldown elapses, a1 can fire again.
	later := base.Add(60*time.Second + 2*time.Minute + time.Second)
	ring.Add(later.Add(-60*time.Second), 0.55) // seed a same-price base 60s before "later"
	if _, _, fired := d.Check("a1", later, ring, 0.70); !fired {
		t.Fatalf("Check() did not fire again after the cooldown elapsed")
	}
}

func TestSurgeDetectorNoHistory(t *testing.T) {
	d := NewSurgeDetector(5, 60*time.Second, 2*time.Minute)
	ring := NewRingBuffer()
	if _, _, fired := d.Check("a1", time.Now(), ring, 0.5); fired {
		t.Fatalf("Check() fired with an empty ring (no history), want no fire")
	}
}
