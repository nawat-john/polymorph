package model

import (
	"encoding/json"
	"testing"
)

// These fixtures are the exact example payloads from design-plan.md section 5
// (not files under testdata/, since section 5 documents the schema inline).

func TestRawEventSchemaMatchesDesignPlan(t *testing.T) {
	const src = `{
		"v": 1,
		"src": "polymarket",
		"kind": "quote",
		"asset_id": "7184...",
		"market_id": "0xabc...",
		"price": 0.634,
		"best_bid": 0.63,
		"best_ask": 0.638,
		"size": 120.5,
		"side": "BUY",
		"src_ts": 1790000000123,
		"recv_ts": 1790000000140
	}`

	var got RawEvent
	if err := json.Unmarshal([]byte(src), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := RawEvent{
		V: 1, Src: "polymarket", Kind: KindQuote,
		AssetID: "7184...", MarketID: "0xabc...",
		Price: 0.634, BestBid: 0.63, BestAsk: 0.638,
		Size: 120.5, Side: "BUY",
		SrcTS: 1790000000123, RecvTS: 1790000000140,
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	// Round trip: re-marshal and unmarshal, expect the same value back.
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back RawEvent
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal round-trip: %v", err)
	}
	if back != got {
		t.Fatalf("round trip mismatch: got %+v, want %+v", back, got)
	}
}

func TestTickSchemaMatchesDesignPlan(t *testing.T) {
	const src = `{
		"v": 1,
		"a": "7184...",
		"m": "0xabc...",
		"p": 0.634,
		"b": 0.63,
		"k": 0.638,
		"c1m": 1.2,
		"c5m": -3.4,
		"vol": 0.021,
		"seq": 88123,
		"rts": 1790000000140,
		"pts": 1790000000152
	}`
	var got Tick
	if err := json.Unmarshal([]byte(src), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := Tick{
		V: 1, AssetID: "7184...", MarketID: "0xabc...",
		Price: 0.634, Bid: 0.63, Ask: 0.638,
		Chg1m: 1.2, Chg5m: -3.4, Vol5m: 0.021,
		Seq: 88123, RecvTS: 1790000000140, ProcTS: 1790000000152,
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestAlertSchemaMatchesDesignPlan(t *testing.T) {
	const src = `{
		"v": 1,
		"a": "7184...",
		"m": "0xabc...",
		"q": "Will X happen by Dec 31?",
		"outcome": "Yes",
		"from": 0.41,
		"to": 0.58,
		"window_s": 60,
		"ts": 1790000000152
	}`
	var got Alert
	if err := json.Unmarshal([]byte(src), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := Alert{
		V: 1, AssetID: "7184...", MarketID: "0xabc...",
		Question: "Will X happen by Dec 31?", Outcome: "Yes",
		From: 0.41, To: 0.58, WindowS: 60, TS: 1790000000152,
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestMarketAssetIDs(t *testing.T) {
	m := Market{ClobTokenIDs: []string{"a", "b"}}
	got := m.AssetIDs()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v", got)
	}
}
