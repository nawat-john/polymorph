// Package clobws is the Polymarket CLOB WebSocket client: connection
// sharding, heartbeat, reconnect and the frame parser (design-plan.md section
// 4.1). See docs/polymarket-notes.md for what was actually observed on the
// wire; this parser is written against that, not the docs alone.
package clobws

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/nawat-john/oddspulse/internal/model"
)

// Wire shapes below mirror what was actually captured (see
// internal/polymarket/testdata/clobws-market.sample.ndjson) and, for
// tick_size_change, the documented-only example in
// internal/polymarket/testdata/clobws-events.from-docs.json. All price/size/
// timestamp fields are JSON strings on the wire, never numbers - confirmed
// live.

type wireBookLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type wireBook struct {
	EventType string          `json:"event_type"`
	Market    string          `json:"market"`
	AssetID   string          `json:"asset_id"`
	Bids      []wireBookLevel `json:"bids"`
	Asks      []wireBookLevel `json:"asks"`
	Timestamp string          `json:"timestamp"`
	// LastTradePrice is only present on the initial post-subscribe snapshot,
	// not on later per-asset book deltas - confirmed live.
	LastTradePrice string `json:"last_trade_price"`
}

type wirePriceChangeEntry struct {
	AssetID string `json:"asset_id"`
	Price   string `json:"price"`
	Size    string `json:"size"`
	Side    string `json:"side"`
	BestBid string `json:"best_bid"`
	BestAsk string `json:"best_ask"`
}

type wirePriceChange struct {
	EventType    string                 `json:"event_type"`
	Market       string                 `json:"market"`
	PriceChanges []wirePriceChangeEntry `json:"price_changes"`
	Timestamp    string                 `json:"timestamp"`
}

type wireLastTradePrice struct {
	EventType string `json:"event_type"`
	Market    string `json:"market"`
	AssetID   string `json:"asset_id"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Side      string `json:"side"`
	Timestamp string `json:"timestamp"`
}

// wireTickSizeChange is built from docs only (never observed live); see
// internal/polymarket/testdata/clobws-events.from-docs.json.
type wireTickSizeChange struct {
	EventType   string `json:"event_type"`
	Market      string `json:"market"`
	AssetID     string `json:"asset_id"`
	OldTickSize string `json:"old_tick_size"`
	NewTickSize string `json:"new_tick_size"`
	Timestamp   string `json:"timestamp"`
}

// Parse turns one raw WS text frame into zero or more RawEvents. A frame may
// be a single JSON object or a JSON array of objects (the initial
// post-subscribe book snapshot arrives as an array - confirmed live); the
// caller is expected to have already filtered out the plain-text "PONG"
// heartbeat reply before calling Parse. recvTS is the ingestor's receive time
// (ms epoch), stamped once per frame.
//
// Event types not requested by this ingestor (best_bid_ask, new_market,
// market_resolved - all require custom_feature_enabled, which this client
// never sets) are skipped rather than treated as an error, so an unexpected
// future event type does not take down the whole frame.
func Parse(frame []byte, recvTS int64) ([]model.RawEvent, error) {
	trimmed := bytes.TrimSpace(frame)
	if len(trimmed) == 0 {
		return nil, nil
	}

	var elems []json.RawMessage
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &elems); err != nil {
			return nil, fmt.Errorf("clobws: decode array frame: %w", err)
		}
	} else {
		elems = []json.RawMessage{trimmed}
	}

	var out []model.RawEvent
	for _, raw := range elems {
		var head struct {
			EventType string `json:"event_type"`
		}
		if err := json.Unmarshal(raw, &head); err != nil {
			return nil, fmt.Errorf("clobws: decode event_type: %w", err)
		}
		switch head.EventType {
		case "book":
			ev, err := parseBook(raw, recvTS)
			if err != nil {
				return nil, err
			}
			out = append(out, ev)
		case "price_change":
			evs, err := parsePriceChange(raw, recvTS)
			if err != nil {
				return nil, err
			}
			out = append(out, evs...)
		case "last_trade_price":
			ev, err := parseLastTradePrice(raw, recvTS)
			if err != nil {
				return nil, err
			}
			out = append(out, ev)
		case "tick_size_change":
			ev, err := parseTickSizeChange(raw, recvTS)
			if err != nil {
				return nil, err
			}
			out = append(out, ev)
		}
	}
	return out, nil
}

func parseBook(raw json.RawMessage, recvTS int64) (model.RawEvent, error) {
	var w wireBook
	if err := json.Unmarshal(raw, &w); err != nil {
		return model.RawEvent{}, fmt.Errorf("clobws: decode book: %w", err)
	}

	// Levels are not guaranteed sorted best-first on the wire (confirmed
	// live), so the best bid/ask is the max bid / min ask over all levels.
	var bestBid, bestAsk float64
	for i, l := range w.Bids {
		p := atof(l.Price)
		if i == 0 || p > bestBid {
			bestBid = p
		}
	}
	for i, l := range w.Asks {
		p := atof(l.Price)
		if i == 0 || p < bestAsk {
			bestAsk = p
		}
	}

	price := atof(w.LastTradePrice)
	if price == 0 && bestBid > 0 && bestAsk > 0 {
		price = (bestBid + bestAsk) / 2
	}

	return model.RawEvent{
		V:        model.RawEventVersion,
		Src:      model.SourcePolymarket,
		Kind:     model.KindBook,
		AssetID:  w.AssetID,
		MarketID: w.Market,
		Price:    price,
		BestBid:  bestBid,
		BestAsk:  bestAsk,
		SrcTS:    atoi64(w.Timestamp),
		RecvTS:   recvTS,
	}, nil
}

func parsePriceChange(raw json.RawMessage, recvTS int64) ([]model.RawEvent, error) {
	var w wirePriceChange
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, fmt.Errorf("clobws: decode price_change: %w", err)
	}
	srcTS := atoi64(w.Timestamp)

	out := make([]model.RawEvent, 0, len(w.PriceChanges))
	for _, pc := range w.PriceChanges {
		out = append(out, model.RawEvent{
			V:        model.RawEventVersion,
			Src:      model.SourcePolymarket,
			Kind:     model.KindQuote,
			AssetID:  pc.AssetID,
			MarketID: w.Market,
			Price:    atof(pc.Price),
			BestBid:  atof(pc.BestBid),
			BestAsk:  atof(pc.BestAsk),
			Size:     atof(pc.Size),
			Side:     pc.Side,
			SrcTS:    srcTS,
			RecvTS:   recvTS,
		})
	}
	return out, nil
}

func parseLastTradePrice(raw json.RawMessage, recvTS int64) (model.RawEvent, error) {
	var w wireLastTradePrice
	if err := json.Unmarshal(raw, &w); err != nil {
		return model.RawEvent{}, fmt.Errorf("clobws: decode last_trade_price: %w", err)
	}
	return model.RawEvent{
		V:        model.RawEventVersion,
		Src:      model.SourcePolymarket,
		Kind:     model.KindTrade,
		AssetID:  w.AssetID,
		MarketID: w.Market,
		Price:    atof(w.Price),
		Size:     atof(w.Size),
		Side:     w.Side,
		SrcTS:    atoi64(w.Timestamp),
		RecvTS:   recvTS,
	}, nil
}

func parseTickSizeChange(raw json.RawMessage, recvTS int64) (model.RawEvent, error) {
	var w wireTickSizeChange
	if err := json.Unmarshal(raw, &w); err != nil {
		return model.RawEvent{}, fmt.Errorf("clobws: decode tick_size_change: %w", err)
	}
	return model.RawEvent{
		V:           model.RawEventVersion,
		Src:         model.SourcePolymarket,
		Kind:        model.KindTickSizeChange,
		AssetID:     w.AssetID,
		MarketID:    w.Market,
		OldTickSize: atof(w.OldTickSize),
		NewTickSize: atof(w.NewTickSize),
		SrcTS:       atoi64(w.Timestamp),
		RecvTS:      recvTS,
	}, nil
}

// atof parses a Polymarket wire price/size string, defaulting to 0 on error.
// These fields are always well-formed decimal strings in practice; a stray
// unparseable value is not worth failing the whole frame over.
func atof(s string) float64 {
	if s == "" {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

// atoi64 parses a Polymarket wire millisecond-epoch timestamp string,
// defaulting to 0 on error.
func atoi64(s string) int64 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
