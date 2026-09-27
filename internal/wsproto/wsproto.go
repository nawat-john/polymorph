// Package wsproto defines the gateway <-> browser WebSocket message types
// (design-plan.md section 6).
package wsproto

import "encoding/json"

// ProtoVersion is the protocol version sent in the "hello" message.
const ProtoVersion = 1

// Channel names for the "ch" field of a client sub/unsub message.
const (
	ChAsset  = "asset"
	ChTop    = "top"
	ChAlerts = "alerts"
	ChSys    = "sys"
)

// Client op names ("op" field).
const (
	OpSub   = "sub"
	OpUnsub = "unsub"
	OpPing  = "ping"
)

// Server message type names ("t" field).
const (
	TypeHello = "hello"
	TypeSnap  = "snap"
	TypeTicks = "ticks"
	TypeTop   = "top"
	TypeAlert = "alert"
	TypeSys   = "sys"
	TypePong  = "pong"
	TypeErr   = "err"
)

// ClientMsg is any client -> server message (design-plan.md section 6): sub,
// unsub or ping. Ch/IDs are set for sub/unsub; T is set for ping.
type ClientMsg struct {
	Op  string   `json:"op"`
	Ch  string   `json:"ch,omitempty"`
	IDs []string `json:"ids,omitempty"`
	T   int64    `json:"t,omitempty"`
}

// ServerMsg is any server -> client message (design-plan.md section 6). Only
// the fields relevant to T are populated. D carries a payload that has
// already been JSON-encoded elsewhere (internal/hub encodes a Tick/Alert/
// TopEntry once and shares the bytes across every subscribed client, per
// design-plan.md section 4.3's "pre-encoding" optimization) - hence
// json.RawMessage rather than "any", so building a ServerMsg never
// re-marshals that payload.
type ServerMsg struct {
	T string          `json:"t"`
	D json.RawMessage `json:"d,omitempty"`

	// hello
	Server string `json:"server,omitempty"`
	Proto  int    `json:"proto,omitempty"`

	// err
	Code string `json:"code,omitempty"`
	Msg  string `json:"msg,omitempty"`

	// pong
	T0  int64 `json:"t0,omitempty"`
	Srv int64 `json:"srv,omitempty"`
}

// Encode marshals m. Every field is a simple scalar or already-validated
// json.RawMessage, so this cannot fail in practice; errors are swallowed
// (empty result) rather than propagated, to keep call sites (which send a
// best-effort message on an already-open socket) simple.
func (m ServerMsg) Encode() []byte {
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return b
}

// Hello builds the server's greeting, sent once when a client connects.
func Hello(server string) ServerMsg {
	return ServerMsg{T: TypeHello, Server: server, Proto: ProtoVersion}
}

// Err builds an error reply, e.g. code="too_many_subs".
func Err(code, msg string) ServerMsg {
	return ServerMsg{T: TypeErr, Code: code, Msg: msg}
}

// Pong builds a reply to a client "ping", echoing its t as t0 and reporting
// the server's own clock as srv.
func Pong(t0, srv int64) ServerMsg {
	return ServerMsg{T: TypePong, T0: t0, Srv: srv}
}

// SysStats is the payload of the "sys" channel (design-plan.md section 6),
// broadcast roughly once a second: live client count and throughput/latency
// numbers for the System Stats panel.
type SysStats struct {
	Clients int     `json:"clients"`
	InEPS   float64 `json:"in_eps"`
	OutMPS  float64 `json:"out_mps"`
	P99Ms   float64 `json:"p99_ms"`
}

// Envelope builds a `{"t":msgType,"d":[items...]}` frame by concatenating
// already-encoded item bytes, so callers never re-marshal a payload that is
// about to be shared across many clients (design-plan.md section 4.3's
// pre-encoding optimization). Used for "snap" and "ticks" (arrays of Tick)
// and "top" is instead built with EnvelopeOne since pm.top's D is forwarded
// as one pre-shaped array blob, not built item-by-item here.
func Envelope(msgType string, items [][]byte) []byte {
	n := len(`{"t":"","d":[]}`) + len(msgType)
	for _, it := range items {
		n += len(it) + 1
	}
	buf := make([]byte, 0, n)
	buf = append(buf, `{"t":"`...)
	buf = append(buf, msgType...)
	buf = append(buf, `","d":[`...)
	for i, it := range items {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, it...)
	}
	buf = append(buf, ']', '}')
	return buf
}

// EnvelopeOne builds a `{"t":msgType,"d":item}` frame from one already-encoded
// blob (a single object for "alert"/"sys", or a pre-shaped array for "top").
func EnvelopeOne(msgType string, item []byte) []byte {
	buf := make([]byte, 0, len(`{"t":"","d":}`)+len(msgType)+len(item))
	buf = append(buf, `{"t":"`...)
	buf = append(buf, msgType...)
	buf = append(buf, `","d":`...)
	buf = append(buf, item...)
	buf = append(buf, '}')
	return buf
}
