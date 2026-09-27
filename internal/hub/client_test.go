package hub

import (
	"strings"
	"testing"
)

func TestConflateTickKeepsOnlyLatestPerAsset(t *testing.T) {
	c := newClient(1, newFakeConn())
	c.conflateTick("a1", []byte(`{"a":"a1","seq":1}`))
	c.conflateTick("a1", []byte(`{"a":"a1","seq":2}`)) // same asset within one window -> conflated
	c.conflateTick("a2", []byte(`{"a":"a2","seq":1}`))

	frames := c.drainPending()
	if len(frames) != 1 {
		t.Fatalf("drainPending() = %d frames, want 1 (one ticks envelope)", len(frames))
	}
	got := string(frames[0])
	// Both assets present, but a1 shows only its latest (seq=2) value.
	if want := `"a":"a1","seq":2`; !strings.Contains(got, want) {
		t.Fatalf("frame %s does not contain latest a1 value %q", got, want)
	}
	if strings.Contains(got, `{"a":"a1","seq":1}`) {
		t.Fatalf("frame %s still contains the stale a1 tick", got)
	}

	// Pending state is cleared after drain.
	if frames2 := c.drainPending(); len(frames2) != 0 {
		t.Fatalf("drainPending() after drain = %v, want empty", frames2)
	}
}

func TestConflateTopKeepsOnlyLatest(t *testing.T) {
	c := newClient(1, newFakeConn())
	c.conflateTop([]byte(`[{"a":"x","c5m":1}]`))
	c.conflateTop([]byte(`[{"a":"x","c5m":2}]`))

	frames := c.drainPending()
	if len(frames) != 1 {
		t.Fatalf("drainPending() = %d frames, want 1", len(frames))
	}
	if want := `{"t":"top","d":[{"a":"x","c5m":2}]}`; string(frames[0]) != want {
		t.Fatalf("frame = %s, want %s", frames[0], want)
	}
}

func TestAddSubsEnforcesCap(t *testing.T) {
	c := newClient(1, newFakeConn())

	keys := []ChannelKey{{Ch: "asset", ID: "a1"}, {Ch: "asset", ID: "a2"}, {Ch: "asset", ID: "a3"}}
	added, rejected := c.addSubs(keys, 2)
	if !rejected {
		t.Fatalf("addSubs() with 3 keys and cap 2: rejected = false, want true")
	}
	if len(added) != 0 {
		t.Fatalf("addSubs() rejected but still added %v (want all-or-nothing)", added)
	}

	added, rejected = c.addSubs(keys[:2], 2)
	if rejected {
		t.Fatalf("addSubs() with 2 keys and cap 2: rejected = true, want false")
	}
	if len(added) != 2 {
		t.Fatalf("addSubs() added %d keys, want 2", len(added))
	}

	// A third, distinct key now exceeds the cap (2 existing + 1 new > 2).
	_, rejected = c.addSubs([]ChannelKey{{Ch: "asset", ID: "a3"}}, 2)
	if !rejected {
		t.Fatalf("addSubs() over cap on an already-subscribed client: rejected = false, want true")
	}

	// Re-adding an already-subscribed key is a no-op, not a cap violation.
	added, rejected = c.addSubs([]ChannelKey{{Ch: "asset", ID: "a1"}}, 2)
	if rejected {
		t.Fatalf("addSubs() re-adding an existing key: rejected = true, want false")
	}
	if len(added) != 0 {
		t.Fatalf("addSubs() re-adding an existing key added %v, want none", added)
	}
}

func TestRemoveAndTakeSubs(t *testing.T) {
	c := newClient(1, newFakeConn())
	keys := []ChannelKey{{Ch: "asset", ID: "a1"}, {Ch: "asset", ID: "a2"}}
	c.addSubs(keys, 500)

	removed := c.removeSubs([]ChannelKey{{Ch: "asset", ID: "a1"}, {Ch: "asset", ID: "nope"}})
	if len(removed) != 1 || removed[0].ID != "a1" {
		t.Fatalf("removeSubs() = %v, want just a1", removed)
	}

	remaining := c.takeSubs()
	if len(remaining) != 1 || remaining[0].ID != "a2" {
		t.Fatalf("takeSubs() = %v, want just a2", remaining)
	}
	if again := c.takeSubs(); len(again) != 0 {
		t.Fatalf("takeSubs() called twice = %v, want empty the second time", again)
	}
}
