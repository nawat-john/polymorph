package hub

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/nawat-john/oddspulse/internal/wsproto"
)

// Client is one connected WebSocket client from Hub's perspective: its
// bounded outbound queue, writer goroutine, and per-client conflation buffer
// (design-plan.md section 4.3's "per-client outbound" + "batching +
// conflation").
type Client struct {
	id   uint64
	conn Conn

	out chan []byte // bounded; drained by runWriter

	mu           sync.Mutex
	subs         map[ChannelKey]struct{}
	pendingTicks map[string][]byte // assetID -> latest pre-encoded Tick bytes
	pendingTop   []byte            // latest pre-shaped top-list bytes

	consecutiveFull atomic.Int32

	closeOnce sync.Once
	done      chan struct{}
}

func newClient(id uint64, conn Conn) *Client {
	return &Client{
		id:           id,
		conn:         conn,
		out:          make(chan []byte, outQueueSize),
		subs:         make(map[ChannelKey]struct{}),
		pendingTicks: make(map[string][]byte),
		done:         make(chan struct{}),
	}
}

// ID returns the client's hub-assigned id (unique per process lifetime).
func (c *Client) ID() uint64 { return c.id }

// enqueue attempts a non-blocking send of frame to the outbound queue.
// Returns false if the queue is currently full (backpressure signal).
func (c *Client) enqueue(frame []byte) bool {
	if frame == nil {
		return true
	}
	select {
	case c.out <- frame:
		return true
	default:
		return false
	}
}

// conflateTick records assetID's latest pre-encoded Tick bytes, replacing
// any value already pending for the same asset this flush window (design-
// plan.md 4.3: "only the latest value is sent").
func (c *Client) conflateTick(assetID string, raw []byte) {
	c.mu.Lock()
	c.pendingTicks[assetID] = raw
	c.mu.Unlock()
}

// conflateTop records the latest pre-shaped top-list bytes, replacing
// whatever was pending.
func (c *Client) conflateTop(raw []byte) {
	c.mu.Lock()
	c.pendingTop = raw
	c.mu.Unlock()
}

// drainPending builds this flush cycle's frames (at most one "ticks" and one
// "top" envelope) from whatever was conflated since the last flush, and
// clears the buffer. Returns nil if nothing is pending.
func (c *Client) drainPending() [][]byte {
	c.mu.Lock()
	var frames [][]byte
	if len(c.pendingTicks) > 0 {
		items := make([][]byte, 0, len(c.pendingTicks))
		for _, v := range c.pendingTicks {
			items = append(items, v)
		}
		clear(c.pendingTicks)
		frames = append(frames, wsproto.Envelope(wsproto.TypeTicks, items))
	}
	if c.pendingTop != nil {
		frames = append(frames, wsproto.EnvelopeOne(wsproto.TypeTop, c.pendingTop))
		c.pendingTop = nil
	}
	c.mu.Unlock()
	return frames
}

// addSubs adds keys not already present, rejecting the whole batch (adding
// none of it) if doing so would push the client's subscription count past
// maxSubs (design-plan.md 4.3: cap enforced with an err reply, not a silent
// partial drop). Returns the keys actually added, and whether the request
// was rejected for exceeding the cap.
func (c *Client) addSubs(keys []ChannelKey, maxSubs int) (added []ChannelKey, rejected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newCount := len(c.subs)
	for _, k := range keys {
		if _, ok := c.subs[k]; !ok {
			newCount++
		}
	}
	if maxSubs > 0 && newCount > maxSubs {
		return nil, true
	}
	for _, k := range keys {
		if _, ok := c.subs[k]; !ok {
			c.subs[k] = struct{}{}
			added = append(added, k)
		}
	}
	return added, false
}

// removeSubs removes keys present in c.subs, returning the ones actually
// removed.
func (c *Client) removeSubs(keys []ChannelKey) (removed []ChannelKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		if _, ok := c.subs[k]; ok {
			delete(c.subs, k)
			removed = append(removed, k)
		}
	}
	return removed
}

// takeSubs empties and returns every key the client was subscribed to
// (called once, when the client disconnects).
func (c *Client) takeSubs() []ChannelKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]ChannelKey, 0, len(c.subs))
	for k := range c.subs {
		keys = append(keys, k)
	}
	c.subs = nil
	return keys
}

// runWriter drains c.out and writes each frame to the connection until ctx
// is done, the connection is closed, or a write fails. onWrite is called
// after each successful write with the frame's byte length (metrics);
// onExit is called exactly once when the loop returns, regardless of cause.
func (c *Client) runWriter(ctx context.Context, onWrite func(n int), onExit func()) {
	defer onExit()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case frame, ok := <-c.out:
			if !ok {
				return
			}
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.conn.Write(wctx, frame)
			cancel()
			if err != nil {
				return
			}
			if onWrite != nil {
				onWrite(len(frame))
			}
		}
	}
}

// closeConn closes the underlying connection with the given WS close
// code/reason exactly once; safe to call multiple times or concurrently.
func (c *Client) closeConn(code int, reason string) {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close(code, reason)
	})
}
