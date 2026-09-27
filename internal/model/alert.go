package model

// AlertVersion is the current Alert.V value.
const AlertVersion = 1

// Alert is the schema of topic pm.alerts (design-plan.md section 5): a surge
// notification, i.e. a price change beyond a threshold within a time window.
type Alert struct {
	V        int     `json:"v"`
	AssetID  string  `json:"a"`
	MarketID string  `json:"m"`
	Question string  `json:"q"`
	Outcome  string  `json:"outcome"`
	From     float64 `json:"from"`
	To       float64 `json:"to"`
	WindowS  int     `json:"window_s"`
	TS       int64   `json:"ts"`
}
