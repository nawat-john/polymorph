package main

import (
	"context"

	"github.com/coder/websocket"
)

// wsConn adapts *coder/websocket.Conn to internal/hub.Conn, so that package
// stays free of any WS library dependency (design-plan.md 12: the ingestor's
// outbound client already uses coder/websocket; this reuses it server-side
// for consistency).
type wsConn struct{ c *websocket.Conn }

func (w wsConn) Write(ctx context.Context, data []byte) error {
	return w.c.Write(ctx, websocket.MessageText, data)
}

func (w wsConn) Close(code int, reason string) error {
	return w.c.Close(websocket.StatusCode(code), reason)
}
