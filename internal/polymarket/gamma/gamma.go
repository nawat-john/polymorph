// Package gamma is a REST client for the Polymarket Gamma API, used for
// market discovery (design-plan.md section 4.1). See docs/polymarket-notes.md
// for the real, captured shape of GET /markets that this package parses.
package gamma

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"

	"github.com/nawat-john/oddspulse/internal/model"
)

// DefaultPageSize is the number of markets requested per page.
const DefaultPageSize = 500

// wireMarket mirrors the real Gamma /markets response fields this package
// needs (internal/polymarket/testdata/gamma-markets.sample.json is a verbatim
// capture). clobTokenIds/outcomes are JSON-encoded strings, not JSON arrays,
// confirmed against the live sample - hence the string type and the second
// json.Unmarshal in toModel.
type wireMarket struct {
	ID           string  `json:"id"`
	ConditionID  string  `json:"conditionId"`
	Question     string  `json:"question"`
	Slug         string  `json:"slug"`
	EndDate      string  `json:"endDate"`
	VolumeNum    float64 `json:"volumeNum"`
	Active       bool    `json:"active"`
	Closed       bool    `json:"closed"`
	Outcomes     string  `json:"outcomes"`
	ClobTokenIDs string  `json:"clobTokenIds"`
}

// toModel converts a Gamma wire market into the pm.markets schema. MarketID
// is set to conditionId, not Gamma's own numeric id: the RawEvent.market_id
// that arrives over the CLOB WS (see clobws' "market" field, e.g.
// "0xabc..." in design-plan.md's own example) is the condition id, confirmed
// live by matching internal/polymarket/testdata/gamma-markets.sample.json's
// conditionId values against clobws-market.sample.ndjson's "market" field.
// Using the same id here is what lets pm.raw/pm.ticks join back to pm.markets.
func (w wireMarket) toModel() (model.Market, error) {
	m := model.Market{
		V:        model.MarketVersion,
		MarketID: w.ConditionID,
		Question: w.Question,
		Slug:     w.Slug,
		EndDate:  w.EndDate,
		Volume:   w.VolumeNum,
		Active:   w.Active,
		Closed:   w.Closed,
	}
	if w.Outcomes != "" {
		if err := json.Unmarshal([]byte(w.Outcomes), &m.Outcomes); err != nil {
			return model.Market{}, fmt.Errorf("gamma: decode outcomes for market %s: %w", w.ID, err)
		}
	}
	if w.ClobTokenIDs != "" {
		if err := json.Unmarshal([]byte(w.ClobTokenIDs), &m.ClobTokenIDs); err != nil {
			return model.Market{}, fmt.Errorf("gamma: decode clobTokenIds for market %s: %w", w.ID, err)
		}
	}
	return m, nil
}

// Client fetches active markets from the Gamma API.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	PageSize   int
}

// NewClient returns a Client with sane defaults; baseURL is typically
// PM_GAMMA_URL (design-plan.md section 13), e.g. https://gamma-api.polymarket.com.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL:    baseURL,
		HTTPClient: http.DefaultClient,
		PageSize:   DefaultPageSize,
	}
}

// FetchActiveMarkets pages through GET /markets?active=true&closed=false
// until a short page is returned, decoding every market into model.Market.
func (c *Client) FetchActiveMarkets(ctx context.Context) ([]model.Market, error) {
	pageSize := c.PageSize
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}

	var out []model.Market
	for offset := 0; ; offset += pageSize {
		page, err := c.fetchPage(ctx, offset, pageSize)
		if err != nil {
			return nil, err
		}
		for _, w := range page {
			m, err := w.toModel()
			if err != nil {
				return nil, err
			}
			out = append(out, m)
		}
		if len(page) < pageSize {
			return out, nil
		}
	}
}

func (c *Client) fetchPage(ctx context.Context, offset, limit int) ([]wireMarket, error) {
	u, err := url.Parse(c.BaseURL + "/markets")
	if err != nil {
		return nil, fmt.Errorf("gamma: parse base url: %w", err)
	}
	q := u.Query()
	q.Set("active", "true")
	q.Set("closed", "false")
	q.Set("limit", fmt.Sprint(limit))
	q.Set("offset", fmt.Sprint(offset))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("gamma: build request: %w", err)
	}

	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gamma: request markets: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("gamma: markets: HTTP %d: %s", resp.StatusCode, body)
	}

	var page []wireMarket
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, fmt.Errorf("gamma: decode markets page: %w", err)
	}
	return page, nil
}

// TopNByVolume returns (a copy of) the n markets with the highest volume,
// highest first. n <= 0 or n >= len(markets) returns every market sorted.
func TopNByVolume(markets []model.Market, n int) []model.Market {
	sorted := make([]model.Market, len(markets))
	copy(sorted, markets)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Volume > sorted[j].Volume })
	if n > 0 && n < len(sorted) {
		sorted = sorted[:n]
	}
	return sorted
}
