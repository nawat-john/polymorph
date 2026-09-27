// Package hub is the gateway's fan-out engine (design-plan.md section 4.3):
// a sharded subscription index, a per-asset/per-top-list cache for
// snapshot-on-subscribe, and per-client batching/conflation with bounded
// outbound queues and slow-client eviction.
//
// Ticks and alerts arrive from Kafka already JSON-encoded (that is literally
// what is stored on pm.ticks/pm.alerts); Hub never re-marshals that payload -
// it shares the same byte slice across every subscribed client's outbound
// frame, per design-plan.md's "pre-encoding" optimization.
package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nawat-john/oddspulse/internal/wsproto"
)

// outQueueSize bounds each client's outbound channel (design-plan.md 4.3:
// "bounded channel per client"). Not exposed as an env var: GW_FLUSH_MS and
// GW_MAX_SUBS are the tunables design-plan.md section 13 calls out; this is
// an internal implementation detail of the backpressure mechanism.
const outQueueSize = 64

// evictAfterMisses: a client whose outbound queue is still full after this
// many consecutive flush cycles is evicted (design-plan.md 4.3).
const evictAfterMisses = 3

// writeTimeout bounds a single frame write to a client; a write that hangs
// this long is treated as a dead/stuck connection.
const writeTimeout = 5 * time.Second

// Conn is the minimal WebSocket connection surface Hub needs. cmd/gateway
// adapts *coder/websocket.Conn to this interface, which keeps this package
// (and its tests) free of any WS library dependency.
type Conn interface {
	Write(ctx context.Context, data []byte) error
	Close(code int, reason string) error
}

// Metrics is the subset of gateway metrics (internal/metrics) Hub updates,
// kept as an interface so this package stays decoupled from
// prometheus/client_golang (the same pattern internal/polymarket/clobws uses
// for its Metrics interface).
type Metrics interface {
	SetClients(n int)
	SetSubscriptions(n int64)
	AddMessagesOut(n int)
	AddBytesOut(n int)
	IncSlowClientEvictions()
	SetQueueDepth(n int)
}

// Config configures a Hub. Values line up with the GW_* env vars in
// design-plan.md section 13.
type Config struct {
	FlushInterval time.Duration // GW_FLUSH_MS
	MaxSubs       int           // GW_MAX_SUBS
	ServerName    string        // sent in the "hello" message

	// NaiveRemarshal (GW_NAIVE_REMARSHAL): measurement harness only, for
	// design-plan.md section 9's "encode once and share bytes vs. encode per
	// client" before/after comparison. When true, PublishTick decodes+
	// re-encodes the tick JSON once per subscribed client instead of sharing
	// the single pre-encoded byte slice - reproducing the CPU cost a
	// per-client-encode design would pay, so it can be measured against the
	// real (default false) pre-encoding path. Never set true outside a
	// benchmark run.
	NaiveRemarshal bool
}

// Hub is the gateway's fan-out engine. Zero value is not usable; use NewHub.
type Hub struct {
	cfg     Config
	metrics Metrics
	log     *slog.Logger

	idx   *index
	cache *cache

	clientsMu sync.RWMutex
	clients   map[uint64]*Client
	nextID    atomic.Uint64

	subCount atomic.Int64 // total subscriptions across all clients
	msgsOut  atomic.Int64 // total frames written to client sockets
}

// NewHub returns a ready-to-use Hub. Call Run in a goroutine to start the
// flush/eviction loop.
func NewHub(cfg Config, metrics Metrics, log *slog.Logger) *Hub {
	if log == nil {
		log = slog.Default()
	}
	return &Hub{
		cfg:     cfg,
		metrics: metrics,
		log:     log,
		idx:     newIndex(),
		cache:   newCache(),
		clients: make(map[uint64]*Client),
	}
}

// Run drives the periodic flush + slow-client-eviction loop until ctx is
// done (design-plan.md 4.3: "flushed every 100ms").
func (h *Hub) Run(ctx context.Context) {
	t := time.NewTicker(h.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.flushAll()
		}
	}
}

func (h *Hub) flushAll() {
	h.clientsMu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.clientsMu.RUnlock()

	depth := 0
	for _, c := range clients {
		h.tickClient(c)
		depth += len(c.out)
	}
	if h.metrics != nil {
		h.metrics.SetQueueDepth(depth)
	}
}

// tickClient flushes one client's conflated ticks/top (if any) and applies
// the slow-client-eviction check (design-plan.md 4.3: "if a client's
// outbound queue is full for N consecutive flush cycles, evict it").
func (h *Hub) tickClient(c *Client) {
	frames := c.drainPending()

	full := false
	for _, f := range frames {
		if !c.enqueue(f) {
			full = true
		}
	}
	if len(frames) == 0 && len(c.out) == cap(c.out) {
		full = true
	}

	if full {
		if c.consecutiveFull.Add(1) >= evictAfterMisses {
			h.evict(c, statusTryAgainLater, "slow client: outbound queue full")
		}
	} else {
		c.consecutiveFull.Store(0)
	}
}

// statusTryAgainLater is RFC 6455 close code 1013, used for slow-client
// eviction (design-plan.md 4.3: "close with an appropriate WS close code").
// Plain int, not a coder/websocket.StatusCode, so this package stays
// WS-library-agnostic; cmd/gateway's Conn adapter converts it back.
const statusTryAgainLater = 1013

// NewClient registers a new client and starts its writer goroutine, which
// runs until ctx is done or the connection errors. Callers (cmd/gateway's
// /ws handler) should derive ctx per-connection so it can be canceled when
// the client's read loop ends.
func (h *Hub) NewClient(ctx context.Context, conn Conn) *Client {
	id := h.nextID.Add(1)
	c := newClient(id, conn)

	h.clientsMu.Lock()
	h.clients[id] = c
	n := len(h.clients)
	h.clientsMu.Unlock()

	if h.metrics != nil {
		h.metrics.SetClients(n)
	}

	go c.runWriter(ctx, func(frameLen int) {
		h.msgsOut.Add(1)
		if h.metrics != nil {
			h.metrics.AddMessagesOut(1)
			h.metrics.AddBytesOut(frameLen)
		}
	}, func() {
		h.RemoveClient(c)
	})
	return c
}

// RemoveClient unregisters c: removes it from the subscription index and the
// clients map. Idempotent (a client already removed is a no-op). It does not
// close the connection - the caller (the /ws read loop, or evict) owns that.
func (h *Hub) RemoveClient(c *Client) {
	h.clientsMu.Lock()
	if _, ok := h.clients[c.id]; !ok {
		h.clientsMu.Unlock()
		return
	}
	delete(h.clients, c.id)
	n := len(h.clients)
	h.clientsMu.Unlock()

	keys := c.takeSubs()
	for _, k := range keys {
		h.idx.remove(k, c)
	}
	if len(keys) > 0 {
		h.addSubCount(-int64(len(keys)))
	}
	if h.metrics != nil {
		h.metrics.SetClients(n)
	}
}

func (h *Hub) evict(c *Client, code int, reason string) {
	h.RemoveClient(c)
	c.closeConn(code, reason)
	if h.metrics != nil {
		h.metrics.IncSlowClientEvictions()
	}
	h.log.Warn("evicted client", "client_id", c.id, "reason", reason)
}

// CloseAll closes every currently connected client with the given WS close
// code/reason (design-plan.md 4.3: graceful shutdown sends code 1012).
func (h *Hub) CloseAll(code int, reason string) {
	h.clientsMu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.clientsMu.RUnlock()
	for _, c := range clients {
		c.closeConn(code, reason)
	}
}

// ClientCount returns the current number of connected clients.
func (h *Hub) ClientCount() int {
	h.clientsMu.RLock()
	defer h.clientsMu.RUnlock()
	return len(h.clients)
}

// MessagesOut returns the running total of frames written to client sockets,
// used by cmd/gateway to compute out_mps for the "sys" channel.
func (h *Hub) MessagesOut() int64 { return h.msgsOut.Load() }

func (h *Hub) addSubCount(delta int64) {
	n := h.subCount.Add(delta)
	if h.metrics != nil {
		h.metrics.SetSubscriptions(n)
	}
}

// SendHello sends the initial greeting (design-plan.md section 6).
func (h *Hub) SendHello(c *Client) {
	c.enqueue(wsproto.Hello(h.cfg.ServerName).Encode())
}

// Pong replies to a client "ping" (design-plan.md section 6).
func (h *Hub) Pong(c *Client, t0 int64) {
	c.enqueue(wsproto.Pong(t0, time.Now().UnixMilli()).Encode())
}

// SeedCache preloads the asset-tick cache from a bootstrap read of
// pm.snapshots (cmd/gateway does this at startup before serving /ws).
func (h *Hub) SeedCache(assetID string, tickJSON []byte) {
	h.cache.setTick(assetID, tickJSON)
}

// Subscribe adds ch (and, for ch=="asset", one subscription per id) to c's
// subscriptions, enforcing the per-client cap (design-plan.md 4.3: "limit
// subscriptions per client, e.g. 500" - replied as a wsproto "err", never a
// silent drop). On success, subscribing to "asset" immediately sends any
// cached snapshot for the newly subscribed ids (design-plan.md: "snapshot on
// subscribe"), and subscribing to "top" does the same from the cached top
// list, since both are cheap, already-available data.
func (h *Hub) Subscribe(c *Client, ch string, ids []string) {
	keys := keysFor(ch, ids)

	added, rejected := c.addSubs(keys, h.cfg.MaxSubs)
	if rejected {
		c.enqueue(wsproto.Err("too_many_subs", fmt.Sprintf("limit %d", h.cfg.MaxSubs)).Encode())
		return
	}
	for _, k := range added {
		h.idx.add(k, c)
	}
	if len(added) > 0 {
		h.addSubCount(int64(len(added)))
	}

	switch ch {
	case wsproto.ChAsset:
		h.sendAssetSnapshot(c, ids)
	case wsproto.ChTop:
		if b, ok := h.cache.getTop(); ok {
			c.enqueue(wsproto.EnvelopeOne(wsproto.TypeTop, b))
		}
	}
}

func (h *Hub) sendAssetSnapshot(c *Client, ids []string) {
	items := make([][]byte, 0, len(ids))
	for _, id := range ids {
		if b, ok := h.cache.getTick(id); ok {
			items = append(items, b)
		}
	}
	if len(items) == 0 {
		return
	}
	c.enqueue(wsproto.Envelope(wsproto.TypeSnap, items))
}

// Unsubscribe removes ch (and, for ch=="asset", the given ids) from c's
// subscriptions.
func (h *Hub) Unsubscribe(c *Client, ch string, ids []string) {
	keys := keysFor(ch, ids)
	removed := c.removeSubs(keys)
	for _, k := range removed {
		h.idx.remove(k, c)
	}
	if len(removed) > 0 {
		h.addSubCount(-int64(len(removed)))
	}
}

// PublishTick fans out one pre-encoded Tick (raw is the exact bytes read
// from a pm.ticks Kafka record - never re-marshaled) to every client
// subscribed to assetID, and refreshes the snapshot cache for future
// subscribers.
func (h *Hub) PublishTick(assetID string, raw []byte) {
	h.cache.setTick(assetID, raw)
	clients := h.idx.snapshot(ChannelKey{Ch: wsproto.ChAsset, ID: assetID})
	if h.cfg.NaiveRemarshal {
		for _, c := range clients {
			c.conflateTick(assetID, naiveReencode(raw))
		}
		return
	}
	for _, c := range clients {
		c.conflateTick(assetID, raw)
	}
}

// naiveReencode decodes+re-encodes raw, standing in for the CPU cost of a
// per-client JSON encode (see Config.NaiveRemarshal's doc comment). Falls
// back to raw on a decode error, which should not happen for anything that
// round-tripped through json.Marshal to begin with.
func naiveReencode(raw []byte) []byte {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	b, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return b
}

// PublishAlert fans out one pre-encoded Alert (raw is the exact bytes read
// from a pm.alerts Kafka record) to every client subscribed to the "alerts"
// channel. Alerts are sent immediately rather than conflated: they are rare
// and, unlike ticks, are not meant to be deduplicated by asset.
func (h *Hub) PublishAlert(raw []byte) {
	frame := wsproto.EnvelopeOne(wsproto.TypeAlert, raw)
	for _, c := range h.idx.snapshot(ChannelKey{Ch: wsproto.ChAlerts}) {
		c.enqueue(frame)
	}
}

// PublishTop fans out the top-movers list to every client subscribed to
// "top", and caches it for snapshot-on-subscribe. raw must already be the
// pre-shaped `[{"a":...,"c5m":...}, ...]` array (cmd/gateway extracts this
// from the pm.top record's D field once, not per client).
func (h *Hub) PublishTop(raw []byte) {
	h.cache.setTop(raw)
	for _, c := range h.idx.snapshot(ChannelKey{Ch: wsproto.ChTop}) {
		c.conflateTop(raw)
	}
}

// PublishSys fans out the "sys" stats payload (raw is an already-encoded
// wsproto.SysStats) to every client subscribed to "sys".
func (h *Hub) PublishSys(raw []byte) {
	frame := wsproto.EnvelopeOne(wsproto.TypeSys, raw)
	for _, c := range h.idx.snapshot(ChannelKey{Ch: wsproto.ChSys}) {
		c.enqueue(frame)
	}
}

// keysFor expands a (ch, ids) sub/unsub request into subscription-index
// keys: one per asset id for ch=="asset", or a single channel-wide key
// otherwise (top/alerts/sys ignore ids, per design-plan.md section 6's
// examples).
func keysFor(ch string, ids []string) []ChannelKey {
	if ch != wsproto.ChAsset {
		return []ChannelKey{{Ch: ch}}
	}
	keys := make([]ChannelKey, len(ids))
	for i, id := range ids {
		keys[i] = ChannelKey{Ch: wsproto.ChAsset, ID: id}
	}
	return keys
}
