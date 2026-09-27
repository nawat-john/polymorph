package model

// TickVersion is the current Tick.V value.
const TickVersion = 1

// Tick is the schema of topic pm.ticks (design-plan.md section 5): a
// ready-to-use price update, sent hundreds of thousands of times, hence the
// short JSON keys (documented here since they are not self-explanatory).
type Tick struct {
	V        int     `json:"v"`
	AssetID  string  `json:"a"`
	MarketID string  `json:"m"`
	Price    float64 `json:"p"`    // last/mid price
	Bid      float64 `json:"b"`    // best bid
	Ask      float64 `json:"k"`    // best ask
	Chg1m    float64 `json:"c1m"`  // percentage-point change over 1 minute
	Chg5m    float64 `json:"c5m"`  // percentage-point change over 5 minutes
	Vol5m    float64 `json:"vol"`  // 5-minute volatility (stddev of log-returns)
	Seq      int64   `json:"seq"`  // per-asset monotonic sequence number
	RecvTS   int64   `json:"rts"`  // ingestor receive time (ms epoch)
	ProcTS   int64   `json:"pts"`  // processor produce time (ms epoch)
}
