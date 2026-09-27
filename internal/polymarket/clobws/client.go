package clobws

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/coder/websocket"

	"github.com/nawat-john/oddspulse/internal/model"
)

// Config configures the Manager. Values line up with the PM_* env vars in
// design-plan.md section 13.
type Config struct {
	URL           string        // PM_WS_URL
	AssetsPerConn int           // PM_ASSETS_PER_CONN, assets per shard connection
	PingInterval  time.Duration // how often to send the "PING" text frame (docs: every 10s)
	IdleTimeout   time.Duration // treat the connection as dropped if nothing arrives within this
	MinBackoff    time.Duration // design-plan.md 4.1: 500ms -> 30s exponential backoff + jitter
	MaxBackoff    time.Duration
}

// DefaultConfig fills in the values design-plan.md and polymarket-notes.md
// call for.
func DefaultConfig(wsURL string, assetsPerConn int) Config {
	return Config{
		URL:           wsURL,
		AssetsPerConn: assetsPerConn,
		PingInterval:  10 * time.Second,
		IdleTimeout:   30 * time.Second,
		MinBackoff:    500 * time.Millisecond,
		MaxBackoff:    30 * time.Second,
	}
}

// Metrics is the subset of ingestor metrics (internal/metrics) the manager
// updates; kept as an interface so this package does not import metrics
// directly (and so tests can use a no-op/fake).
type Metrics interface {
	SetWSConnections(n int)
	IncEventsTotal(kind string, n int)
	IncReconnects()
}

// Manager owns a set of shard connections, splitting asset ids into groups of
// at most Config.AssetsPerConn assets (design-plan.md 4.1: WebSocket
// sharding). Call SetAssets whenever the subscribed asset set should change;
// events are delivered to onEvents from shard goroutines (concurrently, one
// call at a time per shard - onEvents must be safe to call from multiple
// goroutines if there is more than one shard).
type Manager struct {
	cfg      Config
	log      *slog.Logger
	metrics  Metrics
	onEvents func([]model.RawEvent)

	cancelShards []context.CancelFunc
	shardAssets  [][]string
}

// NewManager returns a Manager with no shards running; call SetAssets to
// start subscribing.
func NewManager(cfg Config, log *slog.Logger, metrics Metrics, onEvents func([]model.RawEvent)) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{cfg: cfg, log: log, metrics: metrics, onEvents: onEvents}
}

// SetAssets updates the subscribed asset set. Per design-plan.md 4.1
// ("diff-based subscribe/unsubscribe... or reconnect the affected shards"),
// this implementation takes the reconnect route: it diffs the *overall*
// asset set against what is currently subscribed, and if anything changed,
// tears down every shard and reconnects with fresh, evenly sized shards.
//
// ponytail: this reshards (and reconnects) everything on any change rather
// than diffing per-shard and sending dynamic subscribe/unsubscribe messages
// for just the delta. At the design-plan's own scale (~1000 assets / ~5
// shards, refreshed every 5 minutes) that is a handful of reconnects every 5
// minutes at most, well within the reconnect backoff budget - not worth the
// extra bookkeeping. Upgrade to per-shard diffing if refreshes become more
// frequent or the reconnect churn shows up in ingestor_reconnects_total.
func (m *Manager) SetAssets(ctx context.Context, assetIDs []string) {
	sorted := append([]string(nil), assetIDs...)
	sort.Strings(sorted)

	if sameAssets(sorted, flatten(m.shardAssets)) {
		return
	}

	m.stopShards()

	perConn := m.cfg.AssetsPerConn
	if perConn <= 0 {
		perConn = len(sorted)
		if perConn == 0 {
			perConn = 1
		}
	}
	var shards [][]string
	for i := 0; i < len(sorted); i += perConn {
		end := i + perConn
		if end > len(sorted) {
			end = len(sorted)
		}
		shards = append(shards, sorted[i:end])
	}
	m.shardAssets = shards

	for i, assets := range shards {
		shardCtx, cancel := context.WithCancel(ctx)
		m.cancelShards = append(m.cancelShards, cancel)
		go m.runShard(shardCtx, i, assets)
	}
	if m.metrics != nil {
		m.metrics.SetWSConnections(len(shards))
	}
}

// Stop tears down every shard connection.
func (m *Manager) Stop() {
	m.stopShards()
	if m.metrics != nil {
		m.metrics.SetWSConnections(0)
	}
}

func (m *Manager) stopShards() {
	for _, cancel := range m.cancelShards {
		cancel()
	}
	m.cancelShards = nil
}

func flatten(shards [][]string) []string {
	var out []string
	for _, s := range shards {
		out = append(out, s...)
	}
	sort.Strings(out)
	return out
}

func sameAssets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// subscribeMessage is the CLOB WS "market" channel subscribe payload
// documented at https://docs.polymarket.com/market-data/websocket/market-channel.
type subscribeMessage struct {
	AssetsIDs []string `json:"assets_ids"`
	Type      string   `json:"type"`
}

// backoffResetThreshold: a shard connected for at least this long before
// failing is treated as "was healthy", resetting the backoff to MinBackoff
// rather than continuing to grow it.
const backoffResetThreshold = 60 * time.Second

// runShard owns one WebSocket connection for a fixed set of assets,
// reconnecting with exponential backoff + jitter until ctx is done.
func (m *Manager) runShard(ctx context.Context, id int, assets []string) {
	backoff := m.cfg.MinBackoff
	for ctx.Err() == nil {
		connectedFor, err := m.runShardOnce(ctx, id, assets)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			m.log.Warn("clobws: shard connection ended", "shard", id, "error", err)
		}
		if connectedFor >= backoffResetThreshold {
			backoff = m.cfg.MinBackoff
		}
		if m.metrics != nil {
			m.metrics.IncReconnects()
		}
		wait := jitter(backoff)
		m.log.Warn("clobws: reconnecting", "shard", id, "backoff", wait)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff *= 2
		if backoff > m.cfg.MaxBackoff {
			backoff = m.cfg.MaxBackoff
		}
	}
}

func jitter(d time.Duration) time.Duration {
	// Full jitter: a random duration in [0, d].
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d)))
}

// runShardOnce runs a single connection attempt to completion, returning how
// long it stayed connected before the returned error ended it.
func (m *Manager) runShardOnce(ctx context.Context, id int, assets []string) (time.Duration, error) {
	conn, _, err := websocket.Dial(ctx, m.cfg.URL, nil)
	if err != nil {
		return 0, err
	}
	defer conn.CloseNow()

	sub := subscribeMessage{AssetsIDs: assets, Type: "market"}
	payload, err := json.Marshal(sub)
	if err != nil {
		return 0, err
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return 0, err
	}
	connectedAt := time.Now()
	m.log.Info("clobws: connected", "shard", id, "assets", len(assets))

	pingCtx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	go m.pingLoop(pingCtx, conn)

	for {
		readCtx, cancel := context.WithTimeout(ctx, m.cfg.IdleTimeout)
		_, data, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return time.Since(connectedAt), err
		}
		if string(data) == "PONG" {
			continue
		}
		recvTS := time.Now().UnixMilli()
		events, err := Parse(data, recvTS)
		if err != nil {
			m.log.Warn("clobws: parse error", "shard", id, "error", err)
			continue
		}
		if len(events) == 0 {
			continue
		}
		if m.metrics != nil {
			byKind := map[string]int{}
			for _, ev := range events {
				byKind[string(ev.Kind)]++
			}
			for kind, n := range byKind {
				m.metrics.IncEventsTotal(kind, n)
			}
		}
		m.onEvents(events)
	}
}

func (m *Manager) pingLoop(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(m.cfg.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Application-level heartbeat: a literal "PING" text frame, not a
			// WS protocol ping - confirmed live (docs/polymarket-notes.md).
			_ = conn.Write(ctx, websocket.MessageText, []byte("PING"))
		}
	}
}
