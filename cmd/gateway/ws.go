package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/nawat-john/oddspulse/internal/wsproto"
)

// maxMessageBytes bounds one client->server WS message (design-plan.md
// section 4.3: "max message size 4 KB").
const maxMessageBytes = 4096

// rateLimitPerSec bounds inbound client messages (design-plan.md 4.3: "rate
// limit messages from clients"). A generous cap for a legitimate client -
// sub/unsub/ping traffic is tiny - so it mainly catches a buggy or abusive
// one.
const rateLimitPerSec = 20

// pingInterval/pongTimeout implement the transport-level keepalive
// (design-plan.md section 6: "the server sends a WS ping every 30s and
// closes the connection if there is no pong within 60s"). This is separate
// from the app-level {"op":"ping"}/{"t":"pong"} JSON messages, which measure
// client-perceived round-trip latency rather than liveness.
const (
	pingInterval = 30 * time.Second
	pongTimeout  = 60 * time.Second
)

// handleWS upgrades the request to a WebSocket, registers the client with
// the hub, and runs its read loop until disconnect (design-plan.md 4.3/6).
func (gw *gatewayServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: gw.cfg.AllowedOrigins,
	})
	if err != nil {
		return // Accept already wrote the HTTP error response
	}
	conn.SetReadLimit(maxMessageBytes)

	// Deliberately NOT derived from gw.appCtx: on shutdown, main sends every
	// client a proper close frame via hub.CloseAll (design-plan.md 4.3's
	// code 1012), and Read below returns once that close handshake reaches
	// this connection. If connCtx were canceled by appCtx directly instead,
	// it would race that graceful close - the read loop erroring out from
	// ctx cancellation and CloseNow (an abrupt, handshake-less close, see
	// its defer below) would very likely win, and clients would see a bare
	// EOF instead of the 1012 close frame (caught by manual testing against
	// the live stack; see the Phase 3 handback report).
	connCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer func() { _ = conn.CloseNow() }()

	client := gw.hub.NewClient(connCtx, wsConn{conn})
	gw.hub.SendHello(client)

	go gw.pingLoop(connCtx, cancel, conn)

	rateWindowStart := time.Now()
	count := 0
	for {
		_, data, err := conn.Read(connCtx)
		if err != nil {
			break
		}

		count++
		if since := time.Since(rateWindowStart); since >= time.Second {
			rateWindowStart = time.Now()
			count = 1
		} else if count > rateLimitPerSec {
			_ = conn.Close(websocket.StatusPolicyViolation, "rate limit exceeded")
			break
		}

		var msg wsproto.ClientMsg
		if json.Unmarshal(data, &msg) != nil {
			continue // ignore malformed messages
		}
		switch msg.Op {
		case wsproto.OpSub:
			gw.hub.Subscribe(client, msg.Ch, msg.IDs)
		case wsproto.OpUnsub:
			gw.hub.Unsubscribe(client, msg.Ch, msg.IDs)
		case wsproto.OpPing:
			gw.hub.Pong(client, msg.T)
		}
	}

	gw.hub.RemoveClient(client)
}

// pingLoop sends a transport-level WS ping every pingInterval and cancels
// the connection if the peer does not pong within pongTimeout.
func (gw *gatewayServer) pingLoop(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, pcancel := context.WithTimeout(ctx, pongTimeout)
			err := conn.Ping(pctx)
			pcancel()
			if err != nil {
				cancel()
				return
			}
		}
	}
}
