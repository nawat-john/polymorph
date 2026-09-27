package model

// MarketVersion is the current Market.V value.
const MarketVersion = 1

// Market is the schema of topic pm.markets (compacted, key = market_id):
// discovery metadata from the Gamma API, per design-plan.md sections 4.1/5.
type Market struct {
	V        int    `json:"v"`
	MarketID string `json:"market_id"`
	Question string `json:"question"`
	Slug     string `json:"slug"`
	EndDate  string `json:"end_date,omitempty"`

	// Category/Tags are named in design-plan.md 4.1 but, per
	// docs/polymarket-notes.md, the live Gamma /markets response has no plain
	// category/tags field on the market object itself (it would come from the
	// nested "events"/"marketMetadata" objects, not explored in Phase 1).
	// Left empty rather than fabricated; a later phase can populate them.
	Category string   `json:"category,omitempty"`
	Tags     []string `json:"tags,omitempty"`

	Volume       float64  `json:"volume"` // volumeNum from Gamma
	Active       bool     `json:"active"`
	Closed       bool     `json:"closed"`
	Outcomes     []string `json:"outcomes,omitempty"`
	ClobTokenIDs []string `json:"clob_token_ids"`
}

// AssetIDs returns the outcome token ids to subscribe to on the CLOB WS.
func (m Market) AssetIDs() []string {
	return m.ClobTokenIDs
}
