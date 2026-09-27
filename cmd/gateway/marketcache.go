package main

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/nawat-john/oddspulse/internal/model"
)

// marketCache is a small in-memory snapshot of pm.markets, loaded once at
// bootstrap (design-plan.md section 4.3 / Phase 3 task list: "consuming ...
// pm.markets from earliest at startup to build the initial cache"). It is
// not part of the WS protocol (section 6 has no market-metadata channel);
// it backs a small GET /markets convenience endpoint so the Phase 4
// frontend has somewhere to fetch market question/slug/outcomes without a
// separate service. Loaded once at startup only, not kept fresh afterwards
// - see the "Deviations" note in the Phase 3 handback report.
type marketCache struct {
	mu sync.RWMutex
	m  map[string]model.Market
}

func newMarketCache() *marketCache {
	return &marketCache{m: make(map[string]model.Market)}
}

func (c *marketCache) set(m model.Market) {
	c.mu.Lock()
	c.m[m.MarketID] = m
	c.mu.Unlock()
}

func (c *marketCache) list() []model.Market {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]model.Market, 0, len(c.m))
	for _, m := range c.m {
		out = append(out, m)
	}
	return out
}

// handleMarkets serves the bootstrap-loaded market metadata as JSON. Not
// part of the WS protocol (design-plan.md section 6) - see marketCache's
// doc comment.
//
// CORS: unlike /ws (which checks Origin via AllowedOrigins during the WS
// handshake), this is a plain GET of public, read-only market metadata -
// same content for every caller, nothing per-client or sensitive - so it is
// served with Access-Control-Allow-Origin: * rather than threading
// GW_ALLOWED_ORIGINS through here too. Found missing during Phase 4 live
// testing: the frontend dev server (localhost:5173) is a different origin
// than the gateway (localhost:8081), and without this header the browser's
// fetch("/markets") silently failed CORS with no response body.
func (gw *gatewayServer) handleMarkets(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_ = json.NewEncoder(w).Encode(gw.markets.list())
}
