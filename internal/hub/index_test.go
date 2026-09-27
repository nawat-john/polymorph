package hub

import (
	"fmt"
	"sync"
	"testing"
)

func TestIndexAddRemoveLookup(t *testing.T) {
	idx := newIndex()
	key := ChannelKey{Ch: "asset", ID: "a1"}
	c1 := &Client{id: 1}
	c2 := &Client{id: 2}

	if got := idx.snapshot(key); len(got) != 0 {
		t.Fatalf("snapshot on empty index = %v, want empty", got)
	}

	idx.add(key, c1)
	idx.add(key, c2)
	if got := idx.count(key); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}

	idx.remove(key, c1)
	if got := idx.count(key); got != 1 {
		t.Fatalf("count after remove = %d, want 1", got)
	}
	snap := idx.snapshot(key)
	if len(snap) != 1 || snap[0] != c2 {
		t.Fatalf("snapshot after remove = %v, want [c2]", snap)
	}

	idx.remove(key, c2)
	if got := idx.count(key); got != 0 {
		t.Fatalf("count after removing last client = %d, want 0", got)
	}
}

func TestIndexDistinctKeysDoNotCollide(t *testing.T) {
	idx := newIndex()
	c := &Client{id: 1}
	idx.add(ChannelKey{Ch: "asset", ID: "a1"}, c)
	if got := idx.count(ChannelKey{Ch: "asset", ID: "a2"}); got != 0 {
		t.Fatalf("unrelated key count = %d, want 0", got)
	}
	if got := idx.count(ChannelKey{Ch: "top"}); got != 0 {
		t.Fatalf("unrelated channel count = %d, want 0", got)
	}
	if got := idx.count(ChannelKey{Ch: "asset", ID: "a1"}); got != 1 {
		t.Fatalf("target key count = %d, want 1", got)
	}
}

// TestIndexConcurrentAccess stress-tests add/remove/snapshot from many
// goroutines at once across a small set of keys, so shards collide and the
// per-shard locking is actually exercised.
//
// ponytail: this machine has no C compiler (CGO_ENABLED=0, no gcc on PATH),
// so `go test -race` cannot run here - the same limitation phases 1-2 noted.
// CI (.github/workflows/ci.yml, Linux) runs `go test -race ./...` and does
// exercise this test under the race detector; locally this is a plain stress
// test (many goroutines, no assertions beyond "did not deadlock/panic and
// left the index in a consistent state").
func TestIndexConcurrentAccess(t *testing.T) {
	idx := newIndex()
	const goroutines = 64
	const opsPerGoroutine = 2000
	const numKeys = 8
	const numClients = 16

	clients := make([]*Client, numClients)
	for i := range clients {
		clients[i] = &Client{id: uint64(i)}
	}
	keys := make([]ChannelKey, numKeys)
	for i := range keys {
		keys[i] = ChannelKey{Ch: "asset", ID: fmt.Sprintf("a%d", i)}
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				k := keys[(g+i)%numKeys]
				c := clients[(g*7+i)%numClients]
				switch i % 3 {
				case 0:
					idx.add(k, c)
				case 1:
					idx.remove(k, c)
				case 2:
					_ = idx.snapshot(k)
				}
			}
		}(g)
	}
	wg.Wait()

	// Final state must be consistent: every client returned by snapshot(key)
	// really is present, and every key's count matches its snapshot length.
	for _, k := range keys {
		snap := idx.snapshot(k)
		if got := idx.count(k); got != len(snap) {
			t.Fatalf("key %v: count=%d but snapshot has %d entries", k, got, len(snap))
		}
	}
}
