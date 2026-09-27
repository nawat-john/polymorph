package hub

import "sync"

// numShards is the number of RWMutex-guarded shards the subscription index
// is split across (design-plan.md 4.3: "a sharded RWMutex (e.g. 64 shards by
// hash) to reduce lock contention").
const numShards = 64

// ChannelKey identifies one subscribable channel: an individual asset
// ("asset", id) or one of the channel-wide feeds ("top"/"alerts"/"sys", no
// id).
type ChannelKey struct {
	Ch string
	ID string
}

// shard returns which of the index's numShards shards key belongs to.
func (k ChannelKey) shard() uint32 {
	// Inline FNV-1a (avoids allocating a hash.Hash32 per lookup, which
	// hash/fnv would require on this hot path).
	const offset32 = 2166136261
	const prime32 = 16777619
	h := uint32(offset32)
	for i := 0; i < len(k.Ch); i++ {
		h ^= uint32(k.Ch[i])
		h *= prime32
	}
	h ^= 0xff // separator, so ("a","b") and ("ab","") hash differently
	for i := 0; i < len(k.ID); i++ {
		h ^= uint32(k.ID[i])
		h *= prime32
	}
	return h % numShards
}

type indexShard struct {
	mu   sync.RWMutex
	subs map[ChannelKey]map[*Client]struct{}
}

// index is the subscription index: channel key -> set of subscribed
// clients, sharded to reduce lock contention across unrelated keys
// (design-plan.md section 4.3).
type index struct {
	shards [numShards]indexShard
}

func newIndex() *index {
	idx := &index{}
	for i := range idx.shards {
		idx.shards[i].subs = make(map[ChannelKey]map[*Client]struct{})
	}
	return idx
}

func (i *index) add(key ChannelKey, c *Client) {
	s := &i.shards[key.shard()]
	s.mu.Lock()
	set, ok := s.subs[key]
	if !ok {
		set = make(map[*Client]struct{})
		s.subs[key] = set
	}
	set[c] = struct{}{}
	s.mu.Unlock()
}

func (i *index) remove(key ChannelKey, c *Client) {
	s := &i.shards[key.shard()]
	s.mu.Lock()
	if set, ok := s.subs[key]; ok {
		delete(set, c)
		if len(set) == 0 {
			delete(s.subs, key)
		}
	}
	s.mu.Unlock()
}

// snapshot returns a copy of the clients currently subscribed to key, safe
// to iterate without holding the shard lock (publishing calls the returned
// clients' methods, which may take other locks).
func (i *index) snapshot(key ChannelKey) []*Client {
	s := &i.shards[key.shard()]
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := s.subs[key]
	if len(set) == 0 {
		return nil
	}
	out := make([]*Client, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	return out
}

// count returns the number of clients currently subscribed to key (used by
// tests; production code tracks subscription totals via Hub.subCount).
func (i *index) count(key ChannelKey) int {
	s := &i.shards[key.shard()]
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subs[key])
}
