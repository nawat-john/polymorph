package wsproto

import (
	"encoding/json"
	"testing"
)

// exact examines that got, once marshaled, is byte-identical to want - the
// exact wire text from design-plan.md section 6.
func exact(t *testing.T, got any, want string) {
	t.Helper()
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != want {
		t.Fatalf("marshal = %s, want %s", b, want)
	}
}

// roundTrip unmarshals want into a fresh T, then re-marshals and checks it
// matches want byte-for-byte.
func roundTrip[T any](t *testing.T, want string) {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(want), &v); err != nil {
		t.Fatalf("unmarshal %s: %v", want, err)
	}
	exact(t, v, want)
}

// Client -> server examples, design-plan.md section 6.
func TestClientMsgRoundTrip(t *testing.T) {
	cases := []string{
		`{"op":"sub","ch":"asset","ids":["7184...","9921..."]}`,
		`{"op":"unsub","ch":"asset","ids":["7184..."]}`,
		`{"op":"sub","ch":"top"}`,
		`{"op":"sub","ch":"alerts"}`,
		`{"op":"sub","ch":"sys"}`,
		`{"op":"ping","t":1790000000000}`,
	}
	for _, want := range cases {
		t.Run(want, func(t *testing.T) {
			roundTrip[ClientMsg](t, want)
		})
	}
}

// Server -> client examples, design-plan.md section 6.
func TestServerMsgRoundTrip(t *testing.T) {
	cases := []string{
		`{"t":"hello","server":"gw-1","proto":1}`,
		`{"t":"snap","d":[{"a":"7184..."}]}`,
		`{"t":"ticks","d":[{"a":"7184..."},{"a":"9921..."}]}`,
		`{"t":"top","d":[{"a":"...","c5m":8.1}]}`,
		`{"t":"alert","d":{"a":"7184..."}}`,
		`{"t":"sys","d":{"clients":10234,"in_eps":41200,"out_mps":1900000,"p99_ms":38}}`,
		`{"t":"pong","t0":1790000000000,"srv":1790000000004}`,
		`{"t":"err","code":"too_many_subs","msg":"limit 500"}`,
	}
	for _, want := range cases {
		t.Run(want, func(t *testing.T) {
			roundTrip[ServerMsg](t, want)
		})
	}
}

func TestHelloErrPongBuilders(t *testing.T) {
	exact(t, Hello("gw-1"), `{"t":"hello","server":"gw-1","proto":1}`)
	exact(t, Err("too_many_subs", "limit 500"), `{"t":"err","code":"too_many_subs","msg":"limit 500"}`)
	exact(t, Pong(1790000000000, 1790000000004), `{"t":"pong","t0":1790000000000,"srv":1790000000004}`)
}

func TestEnvelopeSharesPreEncodedBytes(t *testing.T) {
	a := []byte(`{"a":"1"}`)
	b := []byte(`{"a":"2"}`)
	got := Envelope(TypeTicks, [][]byte{a, b})
	want := `{"t":"ticks","d":[{"a":"1"},{"a":"2"}]}`
	if string(got) != want {
		t.Fatalf("Envelope = %s, want %s", got, want)
	}

	empty := Envelope(TypeSnap, nil)
	if string(empty) != `{"t":"snap","d":[]}` {
		t.Fatalf("Envelope(nil) = %s", empty)
	}
}

func TestEnvelopeOne(t *testing.T) {
	item := []byte(`{"a":"7184...","from":0.41,"to":0.58}`)
	got := EnvelopeOne(TypeAlert, item)
	want := `{"t":"alert","d":{"a":"7184...","from":0.41,"to":0.58}}`
	if string(got) != want {
		t.Fatalf("EnvelopeOne = %s, want %s", got, want)
	}

	arr := []byte(`[{"a":"x","c5m":1.5}]`)
	got = EnvelopeOne(TypeTop, arr)
	want = `{"t":"top","d":[{"a":"x","c5m":1.5}]}`
	if string(got) != want {
		t.Fatalf("EnvelopeOne(array) = %s, want %s", got, want)
	}
}
