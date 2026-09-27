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
	c.mu.Lock()
	c.tick[assetID] = raw
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
