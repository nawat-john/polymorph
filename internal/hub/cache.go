package hub

import "sync"

// cache holds the latest pre-encoded state used for snapshot-on-subscribe
// (design-plan.md section 4.3): one Tick's raw bytes per asset (seeded from
// pm.snapshots at bootstrap, kept fresh by every pm.ticks record), and the
// latest top-movers list.
type cache struct {
	mu   sync.RWMutex
	tick map[string][]byte
	top  []byte
}

func newCache() *cache {
	return &cache{tick: make(map[string][]byte)}
}

func (c *cache) setTick(assetID string, raw []byte) {
	// Copy raw rather than retaining the caller's slice: both call sites
	// (SeedCache, PublishTick) pass a kgo.Record.Value straight from a Kafka
	// fetch, whose backing array is often shared by every record decompressed
	// from the same batch. Caching that slice directly kept the *entire*
	// fetch buffer alive for as long as this asset's entry sat in the cache,
	// even after every sibling record in the batch was otherwise garbage.
	cp := make([]byte, len(raw))
	copy(cp, raw)
	c.mu.Lock()
	c.tick[assetID] = cp
	c.mu.Unlock()
}

func (c *cache) getTick(assetID string) ([]byte, bool) {
	c.mu.RLock()
	b, ok := c.tick[assetID]
	c.mu.RUnlock()
	return b, ok
}

func (c *cache) setTop(raw []byte) {
	c.mu.Lock()
	c.top = raw
	c.mu.Unlock()
}

func (c *cache) getTop() ([]byte, bool) {
	c.mu.RLock()
	b := c.top
	c.mu.RUnlock()
	return b, b != nil
}
