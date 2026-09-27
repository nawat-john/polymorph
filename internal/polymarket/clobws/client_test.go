package clobws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/nawat-john/oddspulse/internal/model"
)

// fakeMetrics is a test double for the Metrics interface.
type fakeMetrics struct {
	mu         sync.Mutex
	wsConns    int
	reconnects int32
}

func newFakeMetrics() *fakeMetrics { return &fakeMetrics{} }
func (f *fakeMetrics) SetWSConnections(n int) {
	f.mu.Lock()
	f.wsConns = n
	f.mu.Unlock()
}
func (f *fakeMetrics) IncEventsTotal(string, int) {}
func (f *fakeMetrics) IncReconnects()             { atomic.AddInt32(&f.reconnects, 1) }
func (f *fakeMetrics) connections() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.wsConns
}

// fakeServer accepts CLOB-market-channel-shaped WS connections: it records
// each connection's subscribed asset ids, sends one synthetic "book" event
// for the first subscribed asset, then replies PONG to PING until the client
// disconnects. Not a real Polymarket server - the wire shapes it exercises
// (subscribe message, PING/PONG) are asserted separately against real
// captures in parser_test.go and polymarket-notes.md.
type fakeServer struct {
	mu   sync.Mutex
	subs [][]string
}

func (s *fakeServer) subscribeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}

func (s *fakeServer) handler(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.CloseNow() }()
	ctx := r.Context()

	_, data, err := conn.Read(ctx)
	if err != nil {
		return
	}
	var sub subscribeMessage
	_ = json.Unmarshal(data, &sub)
	s.mu.Lock()
	s.subs = append(s.subs, sub.AssetsIDs)
	s.mu.Unlock()

	if len(sub.AssetsIDs) > 0 {
		frame := `{"event_type":"book","market":"m","asset_id":"` + sub.AssetsIDs[0] +
			`","bids":[{"price":"0.4","size":"1"}],"asks":[{"price":"0.6","size":"1"}],"timestamp":"1"}`
		if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
			return
		}
	}

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if string(data) == "PING" {
			_ = conn.Write(ctx, websocket.MessageText, []byte("PONG"))
		}
	}
}

func wsURLFor(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func TestManager_ShardsAssetsAndDeliversEvents(t *testing.T) {
	fs := &fakeServer{}
	srv := httptest.NewServer(http.HandlerFunc(fs.handler))
	defer srv.Close()

	var mu sync.Mutex
	var received []model.RawEvent
	onEvents := func(evs []model.RawEvent) {
		mu.Lock()
		received = append(received, evs...)
		mu.Unlock()
	}

	metrics := newFakeMetrics()
	cfg := DefaultConfig(wsURLFor(t, srv), 2) // 2 assets per shard
	mgr := NewManager(cfg, nil, metrics, onEvents)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 5 assets, 2 per shard -> 3 shards (2, 2, 1).
	mgr.SetAssets(ctx, []string{"a1", "a2", "a3", "a4", "a5"})

	waitFor(t, 3*time.Second, func() bool { return metrics.connections() == 3 })
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 3
	})

	mgr.Stop()
	waitFor(t, 3*time.Second, func() bool { return metrics.connections() == 0 })
}

func TestManager_SetAssets_SameSetIsNoop(t *testing.T) {
	fs := &fakeServer{}
	srv := httptest.NewServer(http.HandlerFunc(fs.handler))
	defer srv.Close()

	cfg := DefaultConfig(wsURLFor(t, srv), 10)
	mgr := NewManager(cfg, nil, newFakeMetrics(), func([]model.RawEvent) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assets := []string{"a1", "a2"}
	mgr.SetAssets(ctx, assets)
	waitFor(t, 3*time.Second, func() bool { return fs.subscribeCount() == 1 })

	// Same set (different slice order) again: must not open a new connection.
	mgr.SetAssets(ctx, []string{"a2", "a1"})
	time.Sleep(200 * time.Millisecond)
	if got := fs.subscribeCount(); got != 1 {
		t.Fatalf("subscribeCount = %d, want 1 (no-op on unchanged asset set)", got)
	}

	mgr.Stop()
}

func TestManager_SetAssets_ChangedSetReconnects(t *testing.T) {
	fs := &fakeServer{}
	srv := httptest.NewServer(http.HandlerFunc(fs.handler))
	defer srv.Close()

	cfg := DefaultConfig(wsURLFor(t, srv), 10)
	mgr := NewManager(cfg, nil, newFakeMetrics(), func([]model.RawEvent) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.SetAssets(ctx, []string{"a1", "a2"})
	waitFor(t, 3*time.Second, func() bool { return fs.subscribeCount() == 1 })

	mgr.SetAssets(ctx, []string{"a1", "a2", "a3"})
	waitFor(t, 3*time.Second, func() bool { return fs.subscribeCount() == 2 })

	mgr.Stop()
}
