package model

// TopVersion is the current TopList.V value.
const TopVersion = 1

// TopEntry is one row of topic pm.top's top-movers list (design-plan.md
// sections 4.2/6): asset id and its 5-minute percentage-point change. Field
// names match the "top" channel of the gateway<->browser protocol (section
// 6) since pm.top is forwarded to clients close to verbatim.
type TopEntry struct {
	AssetID string  `json:"a"`
	Chg5m   float64 `json:"c5m"`
}

// TopList is the schema of topic pm.top (design-plan.md section 5): the top
// N movers by |chg_5m|, recomputed and produced roughly once a second, keyed
// "top" (single partition).
type TopList struct {
	V  int        `json:"v"`
	TS int64      `json:"ts"`
	D  []TopEntry `json:"d"`
}
