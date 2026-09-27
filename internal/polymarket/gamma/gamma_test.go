package gamma

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
)

// loadSample decodes the real, live-captured Gamma response saved in Phase 0
// (docs/polymarket-notes.md documents how it was captured).
func loadSample(t *testing.T) []json.RawMessage {
	t.Helper()
	data, err := os.ReadFile("../testdata/gamma-markets.sample.json")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal testdata: %v", err)
	}
	return raw
}

// newSampleServer serves the real captured sample with real limit/offset
// pagination semantics, so FetchActiveMarkets is exercised against a server
// that behaves like the live one, without hitting the network.
func newSampleServer(t *testing.T) *httptest.Server {
	t.Helper()
	sample := loadSample(t)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("active"); got != "true" {
			t.Errorf("active=%q, want true", got)
		}
		if got := r.URL.Query().Get("closed"); got != "false" {
			t.Errorf("closed=%q, want false", got)
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

		end := offset + limit
		if end > len(sample) {
			end = len(sample)
		}
		var page []json.RawMessage
		if offset < len(sample) {
			page = sample[offset:end]
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
	}))
}

func TestFetchActiveMarkets_RealSample(t *testing.T) {
	srv := newSampleServer(t)
	defer srv.Close()

	c := NewClient(srv.URL)
	c.PageSize = 2 // force pagination across the 3-market sample

	markets, err := c.FetchActiveMarkets(context.Background())
	if err != nil {
		t.Fatalf("FetchActiveMarkets: %v", err)
	}
	if len(markets) != 3 {
		t.Fatalf("got %d markets, want 3", len(markets))
	}

	byID := map[string]int{}
	for i, m := range markets {
		byID[m.MarketID] = i
	}

	// Spot-check market 4813637 (Minnesota vs. Washington) against the real
	// captured fields, including the double-JSON-encoded clobTokenIds/outcomes.
	// Keyed by conditionId (see toModel's doc comment for why).
	const conditionID = "0xaace8abeca704f81a4defeb1ef2da5a28874e120167709ec8435ad2726756204"
	i, ok := byID[conditionID]
	if !ok {
		t.Fatalf("market %s not found in %+v", conditionID, markets)
	}
	m := markets[i]
	if m.Question != "Minnesota vs. Washington" {
		t.Errorf("question = %q", m.Question)
	}
	if m.Slug != "cfb-minnst-wash-2026-09-26" {
		t.Errorf("slug = %q", m.Slug)
	}
	if !m.Active || m.Closed {
		t.Errorf("active=%v closed=%v, want true/false", m.Active, m.Closed)
	}
	wantOutcomes := []string{"Minnesota", "Washington"}
	if len(m.Outcomes) != 2 || m.Outcomes[0] != wantOutcomes[0] || m.Outcomes[1] != wantOutcomes[1] {
		t.Errorf("outcomes = %v, want %v", m.Outcomes, wantOutcomes)
	}
	wantTokens := []string{
		"75313892657684104434100329364341570294792244790861570592628402169992331090895",
		"45353365042853804097303208844798871546434854427330868428137263948892838923349",
	}
	if len(m.ClobTokenIDs) != 2 || m.ClobTokenIDs[0] != wantTokens[0] || m.ClobTokenIDs[1] != wantTokens[1] {
		t.Errorf("clobTokenIds = %v, want %v", m.ClobTokenIDs, wantTokens)
	}
	if m.Volume < 1197862 || m.Volume > 1197863 {
		t.Errorf("volume (volumeNum) = %v, want ~1197862.31", m.Volume)
	}
}

func TestTopNByVolume_RealSample(t *testing.T) {
	srv := newSampleServer(t)
	defer srv.Close()

	c := NewClient(srv.URL)
	markets, err := c.FetchActiveMarkets(context.Background())
	if err != nil {
		t.Fatalf("FetchActiveMarkets: %v", err)
	}

	top := TopNByVolume(markets, 2)
	if len(top) != 2 {
		t.Fatalf("got %d, want 2", len(top))
	}
	// Real volumeNum values: 4813637=1197862.31, 4641064=1521804.88,
	// 2772194=22892933.90 - so top 2 by volume are 2772194 then 4641064
	// (market_id is conditionId - see toModel's doc comment).
	const cond2772194 = "0xefa17dee3af09f69f9ddf245b969aa4efbe7c71cdf06ee49d694408bc33e2ed2"
	const cond4641064 = "0x690fafd3b7bb8b4919ccf0f219b0014b65334411a1076162e76353e76d7f9bec"
	if top[0].MarketID != cond2772194 || top[1].MarketID != cond4641064 {
		t.Errorf("got order %s, %s; want %s, %s", top[0].MarketID, top[1].MarketID, cond2772194, cond4641064)
	}
	if top[0].Volume < top[1].Volume {
		t.Errorf("not sorted descending: %v < %v", top[0].Volume, top[1].Volume)
	}
}

func TestFetchActiveMarkets_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	if _, err := c.FetchActiveMarkets(context.Background()); err == nil {
		t.Fatal("expected error, got nil")
	}
}
