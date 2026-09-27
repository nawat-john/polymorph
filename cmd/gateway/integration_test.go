package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/nawat-john/oddspulse/internal/hub"
	"github.com/nawat-john/oddspulse/internal/wsproto"
)

// TestWSIntegration is the Phase 3 checklist's "Integration test": a real
// in-process WebSocket server (net/http/httptest + the actual /ws handler)
// against records injected directly into the hub - no Kafka/Docker needed,
// so this stays fast and hermetic. It asserts a client that subscribes
// receives a "snap" of the cached tick, then a "ticks" update, both within
// the expected flush window.
func TestWSIntegration(t *testing.T) {
	h := hub.NewHub(hub.Config{FlushInterval: 20 * time.Millisecond, MaxSubs: 500, ServerName: "gw-test"}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	gw := &gatewayServer{
		cfg:    Config{AllowedOrigins: nil},
		hub:    h,
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		appCtx: ctx,
	}

	// A tick arrives (as if processed from pm.ticks) before any client
	// connects, seeding the snapshot cache.
	h.PublishTick("a1", []byte(`{"a":"a1","p":0.5,"seq":1}`))

	srv := httptest.NewServer(http.HandlerFunc(gw.handleWS))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, resp, err := websocket.Dial(dialCtx, wsURL, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()

	readMsg := func() wsproto.ServerMsg {
		t.Helper()
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer rcancel()
		_, data, err := conn.Read(rctx)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		var m wsproto.ServerMsg
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("unmarshal %s: %v", data, err)
		}
		return m
	}

	if hello := readMsg(); hello.T != wsproto.TypeHello {
		t.Fatalf("first message = %+v, want type=hello", hello)
	}

	sub := wsproto.ClientMsg{Op: wsproto.OpSub, Ch: wsproto.ChAsset, IDs: []string{"a1"}}
	subBytes, _ := json.Marshal(sub)
	writeCtx, writeCancel := context.WithTimeout(context.Background(), time.Second)
	if err := conn.Write(writeCtx, websocket.MessageText, subBytes); err != nil {
		writeCancel()
		t.Fatalf("Write sub: %v", err)
	}
	writeCancel()

	snap := readMsg()
	if snap.T != wsproto.TypeSnap {
		t.Fatalf("message after subscribe = %+v, want type=snap", snap)
	}
	if string(snap.D) != `[{"a":"a1","p":0.5,"seq":1}]` {
		t.Fatalf("snap payload = %s, want the seeded tick", snap.D)
	}

	// Publish a fresh tick; it must arrive as a "ticks" update within the
	// flush window (a couple of flush intervals of slack).
	h.PublishTick("a1", []byte(`{"a":"a1","p":0.6,"seq":2}`))

	ticks := readMsg()
	if ticks.T != wsproto.TypeTicks {
		t.Fatalf("next message = %+v, want type=ticks", ticks)
	}
	if string(ticks.D) != `[{"a":"a1","p":0.6,"seq":2}]` {
		t.Fatalf("ticks payload = %s, want the published tick", ticks.D)
	}
}
