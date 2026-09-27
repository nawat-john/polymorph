package model

// EventKind is the value of RawEvent.Kind.
type EventKind string

// Values observed from (or documented for) the Polymarket CLOB market channel;
// see docs/polymarket-notes.md for which were captured live vs. docs-only.
const (
	KindQuote EventKind = "quote" // price_change
	KindTrade EventKind = "trade" // last_trade_price
	KindBook  EventKind = "book"  // book (snapshot or delta)
	// KindTickSizeChange is not one of the three kinds design-plan.md section 5
	// enumerates for RawEvent.Kind, but the WS parser must still turn the
	// documented tick_size_change frame into something; it carries no price
	// data so it gets its own kind rather than faking a quote/trade.
	KindTickSizeChange EventKind = "tick_size_change"
)

// SourcePolymarket is the only RawEvent.Src value used by this ingestor today.
const SourcePolymarket = "polymarket"

// RawEventVersion is the current RawEvent.V value.
const RawEventVersion = 1

// RawEvent is the schema of topic pm.raw (design-plan.md section 5): a single,
// lightly normalized Polymarket event, keyed by asset_id when produced.
type RawEvent struct {
	V        int       `json:"v"`
	Src      string    `json:"src"`
	Kind     EventKind `json:"kind"`
	AssetID  string    `json:"asset_id"`
	MarketID string    `json:"market_id,omitempty"`
	Price    float64   `json:"price,omitempty"`
	BestBid  float64   `json:"best_bid,omitempty"`
	BestAsk  float64   `json:"best_ask,omitempty"`
	Size     float64   `json:"size,omitempty"`
	Side     string    `json:"side,omitempty"`

	// OldTickSize/NewTickSize carry a tick_size_change event's payload. They
	// are not part of design-plan.md's example RawEvent JSON, which only
	// documents quote/trade/book; kept as an additive, omitempty extension so
	// every other consumer of pm.raw is unaffected.
	OldTickSize float64 `json:"old_tick_size,omitempty"`
	NewTickSize float64 `json:"new_tick_size,omitempty"`

	SrcTS  int64 `json:"src_ts"`
	RecvTS int64 `json:"recv_ts"`
}
