package hub

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nawat-john/oddspulse/internal/wsproto"
)

func newTestHub(flush time.Duration, maxSubs int) (*Hub, *fakeMetrics) {
	fm := &fakeMetrics{}
	h := NewHub(Config{FlushInterval: flush, MaxSubs: maxSubs, ServerName: "gw-test"}, fm, nil)
	return h, fm
}

func lastFrame(t *testing.T, fc *fakeConn) wsproto.ServerMsg {
	t.Helper()
	frames := fc.waitFrames(t, 1, time.Second)
	var m wsproto.ServerMsg
	if err := json.Unmarshal(frames[len(frames)-1], &m); err != nil {
		t.Fatalf("unmarshal last frame %s: %v", frames[len(frames)-1], err)
	}
	return m
}

func TestSubscribeCapRejectedWithErr(t *testing.T) {
	h, _ := newTestHub(time.Hour, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc := newFakeConn()
	c := h.NewClient(ctx, fc)

	h.Subscribe(c, wsproto.ChAsset, []string{"a1", "a2", "a3"})

	msg := lastFrame(t, fc)
	if msg.T != wsproto.TypeErr || msg.Code != "too_many_subs" {
		t.Fatalf("got %+v, want an err/too_many_subs reply", msg)
	}
}

func TestSnapshotOnSubscribe(t *testing.T) {
	h, _ := newTestHub(time.Hour, 500)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A tick arrives (e.g. from pm.ticks) before anyone subscribes; PublishTick
	// also seeds the cache used for snapshot-on-subscribe.
	h.PublishTick("a1", []byte(`{"a":"a1","p":0.5}`))

	fc := newFakeConn()
	c := h.NewClient(ctx, fc)
	h.Subscribe(c, wsproto.ChAsset, []string{"a1"})

	msg := lastFrame(t, fc)
	if msg.T != wsproto.TypeSnap {
		t.Fatalf("first message after subscribe = %+v, want type=snap", msg)
	}
	if string(msg.D) != `[{"a":"a1","p":0.5}]` {
		t.Fatalf("snap payload = %s, want the cached tick", msg.D)
	}
}

func TestSeedCacheThenSubscribeAlsoSnapshots(t *testing.T) {
	h, _ := newTestHub(time.Hour, 500)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Simulates cmd/gateway's bootstrap-from-pm.snapshots step, run before
	// any client connects.
	h.SeedCache("a1", []byte(`{"a":"a1","p":0.9}`))

	fc := newFakeConn()
	c := h.NewClient(ctx, fc)
	h.Subscribe(c, wsproto.ChAsset, []string{"a1", "unknown-asset"})

	msg := lastFrame(t, fc)
	if msg.T != wsproto.TypeSnap || string(msg.D) != `[{"a":"a1","p":0.9}]` {
		t.Fatalf("got %+v, want a snap containing only the cached a1 tick", msg)
	}
}

func TestPublishTickConflatesAcrossOneFlushWindow(t *testing.T) {
	h, _ := newTestHub(time.Hour, 500) // flush driven manually via flushAll
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc := newFakeConn()
	c := h.NewClient(ctx, fc)
	h.Subscribe(c, wsproto.ChAsset, []string{"a1"})
	fc.mu.Lock()
	fc.frames = nil // drop the snapshot frame (empty cache, none sent) / any setup noise
	fc.mu.Unlock()

	h.PublishTick("a1", []byte(`{"a":"a1","seq":1}`))
	h.PublishTick("a1", []byte(`{"a":"a1","seq":2}`))
	h.PublishTick("a1", []byte(`{"a":"a1","seq":3}`))
	h.flushAll()

	frames := fc.waitFrames(t, 1, time.Second)
	want := `{"t":"ticks","d":[{"a":"a1","seq":3}]}`
	if string(frames[0]) != want {
		t.Fatalf("frame = %s, want %s", frames[0], want)
	}
	// Give any (unwanted) extra writes a moment to land, then confirm there
	// really was only ever the one conflated frame.
	time.Sleep(20 * time.Millisecond)
	if got := len(fc.framesSnapshot()); got != 1 {
		t.Fatalf("frames written = %d, want exactly 1 (conflated ticks envelope)", got)
	}
}

func TestUnsubscribeStopsFanout(t *testing.T) {
	h, _ := newTestHub(time.Hour, 500)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc := newFakeConn()
	c := h.NewClient(ctx, fc)
	h.Subscribe(c, wsproto.ChAsset, []string{"a1"})
	h.Unsubscribe(c, wsproto.ChAsset, []string{"a1"})

	h.PublishTick("a1", []byte(`{"a":"a1"}`))
	h.flushAll()
	time.Sleep(20 * time.Millisecond)

	if got := idxCount(h, wsproto.ChAsset, "a1"); got != 0 {
		t.Fatalf("subscriber count for a1 after unsubscribe = %d, want 0", got)
	}
	frames := fc.framesSnapshot()
	for _, f := range frames {
		if string(f) == `{"t":"ticks","d":[{"a":"a1"}]}` {
			t.Fatalf("client received a tick after unsubscribing: %s", f)
		}
	}
}

func idxCount(h *Hub, ch, id string) int {
	return h.idx.count(ChannelKey{Ch: ch, ID: id})
}

func TestRemoveClientCleansUpSubscriptions(t *testing.T) {
	h, fm := newTestHub(time.Hour, 500)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc := newFakeConn()
	c := h.NewClient(ctx, fc)
	h.Subscribe(c, wsproto.ChAsset, []string{"a1", "a2"})
	if fm.subscriptions.Load() != 2 {
		t.Fatalf("subscriptions gauge = %d, want 2", fm.subscriptions.Load())
	}

	h.RemoveClient(c)
	if fm.subscriptions.Load() != 0 {
		t.Fatalf("subscriptions gauge after RemoveClient = %d, want 0", fm.subscriptions.Load())
	}
	if got := idxCount(h, wsproto.ChAsset, "a1"); got != 0 {
		t.Fatalf("index still has a1 subscriber after RemoveClient")
	}
	if h.ClientCount() != 0 {
		t.Fatalf("ClientCount() = %d, want 0", h.ClientCount())
	}
}

// TestSlowClientEviction exercises the real Hub.Run flush loop against a
// client whose connection never finishes a write (simulating a slow
// consumer): design-plan.md 4.3's "if a client's outbound queue is full for
// N consecutive flush cycles, evict it" backpressure rule.
func TestSlowClientEviction(t *testing.T) {
	h, fm := newTestHub(2*time.Millisecond, 500)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	fc := newBlockingConn()
	defer fc.unblock()
	c := h.NewClient(ctx, fc)
	h.Subscribe(c, wsproto.ChAsset, []string{"a1"})

	deadline := time.After(3 * time.Second)
	pace := time.NewTicker(time.Millisecond)
	defer pace.Stop()
	for fm.evictions.Load() == 0 {
		h.PublishTick("a1", []byte(`{"a":"a1"}`))
		select {
		case <-deadline:
			t.Fatalf("client was not evicted within the deadline")
		case <-pace.C:
		}
	}

	if got := fm.evictions.Load(); got != 1 {
		t.Fatalf("evictions = %d, want 1", got)
	}
	if !fc.closed.Load() {
		t.Fatalf("evicted client's connection was not closed")
	}
	if got := fc.code.Load(); got != statusTryAgainLater {
		t.Fatalf("close code = %d, want %d", got, statusTryAgainLater)
	}
	if h.ClientCount() != 0 {
		t.Fatalf("ClientCount() after eviction = %d, want 0", h.ClientCount())
	}
}

// TestWriteErrorEvictsAndCountsMetric exercises the *other* slow-client
// disconnect path found in S4 testing (docs/benchmark.md): a write that
// fails/times out at the connection level, as opposed to
// TestSlowClientEviction's queue-still-full-after-N-flushes path. Both must
// increment gateway_slow_client_evictions_total.
func TestWriteErrorEvictsAndCountsMetric(t *testing.T) {
	h, fm := newTestHub(time.Hour, 500) // no periodic flush needed: SendHello enqueues directly
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc := newFailingConn(errors.New("simulated write timeout"))
	c := h.NewClient(ctx, fc)
	h.SendHello(c) // gives the writer goroutine a frame to fail on

	deadline := time.After(time.Second)
	for fm.evictions.Load() == 0 {
		select {
		case <-deadline:
			t.Fatalf("write error did not trigger an eviction / metric increment")
		case <-time.After(time.Millisecond):
		}
	}
	if got := fm.evictions.Load(); got != 1 {
		t.Fatalf("evictions = %d, want 1", got)
	}
	if h.ClientCount() != 0 {
		t.Fatalf("ClientCount() after write-error eviction = %d, want 0", h.ClientCount())
	}
}

func TestPublishAlertAndTopAndSys(t *testing.T) {
	h, _ := newTestHub(time.Hour, 500)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alertClient := newFakeConn()
	ac := h.NewClient(ctx, alertClient)
	h.Subscribe(ac, wsproto.ChAlerts, nil)

	topClient := newFakeConn()
	tc := h.NewClient(ctx, topClient)
	h.Subscribe(tc, wsproto.ChTop, nil)

	sysClient := newFakeConn()
	sc := h.NewClient(ctx, sysClient)
	h.Subscribe(sc, wsproto.ChSys, nil)

	h.PublishAlert([]byte(`{"a":"a1","from":0.4,"to":0.6}`))
	h.PublishTop([]byte(`[{"a":"a1","c5m":8.1}]`))
	h.flushAll() // top is conflated; alert/sys are sent immediately but this is harmless
	h.PublishSys([]byte(`{"clients":1,"in_eps":0,"out_mps":0,"p99_ms":0}`))

	if got := lastFrame(t, alertClient); got.T != wsproto.TypeAlert {
		t.Fatalf("alert subscriber got %+v, want type=alert", got)
	}
	if got := lastFrame(t, topClient); got.T != wsproto.TypeTop || string(got.D) != `[{"a":"a1","c5m":8.1}]` {
		t.Fatalf("top subscriber got %+v", got)
	}
	if got := lastFrame(t, sysClient); got.T != wsproto.TypeSys {
		t.Fatalf("sys subscriber got %+v, want type=sys", got)
	}
}
