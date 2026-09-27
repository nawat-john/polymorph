package clobws

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"

	"github.com/nawat-john/oddspulse/internal/model"
)

// readSampleFrames reads the real, live-captured WS frames (one per line;
// docs/polymarket-notes.md documents how they were captured: subscribed to 6
// real asset ids and recorded ~20s of traffic verbatim).
func readSampleFrames(t *testing.T) []string {
	t.Helper()
	f, err := os.Open("../testdata/clobws-market.sample.ndjson")
	if err != nil {
		t.Fatalf("open testdata: %v", err)
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		if line := sc.Text(); line != "" {
			lines = append(lines, line)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan testdata: %v", err)
	}
	return lines
}

// TestParse_LiveCapturedFrames replays every frame from the real Phase 0
// capture and checks the parser accounts for all 65 events across the 60
// frames (26 book, 29 price_change with 2 entries each, 10 last_trade_price)
// without error, per docs/polymarket-notes.md's recorded event_types counts.
func TestParse_LiveCapturedFrames(t *testing.T) {
	frames := readSampleFrames(t)
	if len(frames) != 60 {
		t.Fatalf("got %d frames, want 60 (see probe-summary.json)", len(frames))
	}

	counts := map[model.EventKind]int{}
	for i, frame := range frames {
		events, err := Parse([]byte(frame), 1234567890)
		if err != nil {
			t.Fatalf("frame %d: Parse: %v", i, err)
		}
		for _, ev := range events {
			counts[ev.Kind]++
			if ev.AssetID == "" {
				t.Errorf("frame %d: event with empty asset_id: %+v", i, ev)
			}
			if ev.V != model.RawEventVersion || ev.Src != model.SourcePolymarket {
				t.Errorf("frame %d: v/src = %d/%q", i, ev.V, ev.Src)
			}
		}
	}

	// probe-summary.json: book=26, price_change=29 (each with 2 price_changes
	// entries -> 58 quote RawEvents), last_trade_price=10.
	if counts[model.KindBook] != 26 {
		t.Errorf("book count = %d, want 26", counts[model.KindBook])
	}
	if counts[model.KindQuote] != 58 {
		t.Errorf("quote count = %d, want 58 (29 price_change frames x 2 entries)", counts[model.KindQuote])
	}
	if counts[model.KindTrade] != 10 {
		t.Errorf("trade count = %d, want 10", counts[model.KindTrade])
	}
}

// TestParse_InitialSnapshotArray checks the very first captured frame: a JSON
// array of 6 book objects sent as the post-subscribe snapshot (one per
// subscribed asset), with the array's first element carrying the
// snapshot-only tick_size/last_trade_price fields - both confirmed live.
func TestParse_InitialSnapshotArray(t *testing.T) {
	frames := readSampleFrames(t)
	events, err := Parse([]byte(frames[0]), 42)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 6 {
		t.Fatalf("got %d events, want 6", len(events))
	}
	first := events[0]
	if first.Kind != model.KindBook {
		t.Errorf("kind = %q, want book", first.Kind)
	}
	if first.AssetID != "18108354744468601294025853601030425188395211927926870885542758981304523217919" {
		t.Errorf("asset_id = %q", first.AssetID)
	}
	// last_trade_price on the wire is "0.870"; best_bid/best_ask are the
	// max bid / min ask over unsorted levels, not the first element.
	if first.Price != 0.870 {
		t.Errorf("price = %v, want 0.870 (from last_trade_price)", first.Price)
	}
	if first.BestBid != 0.87 {
		t.Errorf("best_bid = %v, want 0.87 (max bid level)", first.BestBid)
	}
	if first.BestAsk != 0.88 {
		t.Errorf("best_ask = %v, want 0.88 (min ask level)", first.BestAsk)
	}
	if first.RecvTS != 42 {
		t.Errorf("recv_ts = %d, want 42", first.RecvTS)
	}
}

// TestParse_LastTradePriceFrame checks one concrete last_trade_price object
// against its known real field values (captured live).
func TestParse_LastTradePriceFrame(t *testing.T) {
	const frame = `{"market":"0xefa17dee3af09f69f9ddf245b969aa4efbe7c71cdf06ee49d694408bc33e2ed2","asset_id":"33339798372916037220786136133406478491116150628220441951753426683441228279665","price":"0.002","size":"5","fee_rate_bps":"0","side":"BUY","timestamp":"1790491136979","event_type":"last_trade_price","transaction_hash":"0x220c4bc4394b2d989e7c2c4b74337f9aa1b010ced31505ad140deb61b8104f7f"}`

	events, err := Parse([]byte(frame), 99)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]
	if ev.Kind != model.KindTrade {
		t.Errorf("kind = %q, want trade", ev.Kind)
	}
	if ev.Price != 0.002 || ev.Size != 5 || ev.Side != "BUY" {
		t.Errorf("got price=%v size=%v side=%v", ev.Price, ev.Size, ev.Side)
	}
	if ev.SrcTS != 1790491136979 {
		t.Errorf("src_ts = %d", ev.SrcTS)
	}
}

// TestParse_TickSizeChange_FromDocs parses the docs-only tick_size_change
// example from internal/polymarket/testdata/clobws-events.from-docs.json.
// This event type was never observed live in the Phase 0 capture.
func TestParse_TickSizeChange_FromDocs(t *testing.T) {
	data, err := os.ReadFile("../testdata/clobws-events.from-docs.json")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal testdata: %v", err)
	}
	frame, ok := doc["tick_size_change"]
	if !ok {
		t.Fatal("testdata missing tick_size_change fixture")
	}

	events, err := Parse(frame, 7)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]
	if ev.Kind != model.KindTickSizeChange {
		t.Errorf("kind = %q, want tick_size_change", ev.Kind)
	}
	if ev.OldTickSize != 0.01 || ev.NewTickSize != 0.001 {
		t.Errorf("got old=%v new=%v, want 0.01/0.001", ev.OldTickSize, ev.NewTickSize)
	}
	if ev.AssetID != "107505882767731489358349912513945399560393482969656700824895970500493757150417" {
		t.Errorf("asset_id = %q", ev.AssetID)
	}
}

// TestParse_UnrequestedEventTypesAreSkipped checks that the docs-only events
// requiring custom_feature_enabled (never set by this client) are silently
// skipped rather than erroring, since a frame carrying one should not take
// down the whole read loop.
func TestParse_UnrequestedEventTypesAreSkipped(t *testing.T) {
	data, err := os.ReadFile("../testdata/clobws-events.from-docs.json")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal testdata: %v", err)
	}
	for _, key := range []string{
		"best_bid_ask_requires_custom_feature_enabled",
		"new_market_requires_custom_feature_enabled",
		"market_resolved_requires_custom_feature_enabled",
	} {
		frame, ok := doc[key]
		if !ok {
			t.Fatalf("testdata missing %s fixture", key)
		}
		events, err := Parse(frame, 1)
		if err != nil {
			t.Errorf("%s: Parse: %v", key, err)
		}
		if len(events) != 0 {
			t.Errorf("%s: got %d events, want 0", key, len(events))
		}
	}
}

func TestParse_EmptyFrame(t *testing.T) {
	events, err := Parse([]byte(""), 1)
	if err != nil || events != nil {
		t.Fatalf("got events=%v err=%v, want nil, nil", events, err)
	}
}

func TestParse_InvalidJSON(t *testing.T) {
	if _, err := Parse([]byte("not json"), 1); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
