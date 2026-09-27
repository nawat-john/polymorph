# Polymarket API notes (Phase 0 exploration)

Consulted 2026-09-27, from a residential IP with no VPN. Everything below is either
(a) quoted from the official docs, or (b) captured live and saved verbatim under
`internal/polymarket/testdata/` — each claim says which.

Sources:
- https://docs.polymarket.com/market-data/websocket/market-channel (WebSocket market channel — from docs)
- https://docs.polymarket.com/api-reference/markets/list-markets (Gamma `/markets` — from docs)
- https://gamma-api.polymarket.com/markets (live capture)
- wss://ws-subscriptions-clob.polymarket.com/ws/market (live capture)

No region block or rate limit was hit during this probe. That does not guarantee the
production VPS location will see the same result — re-check once the VPS is chosen (§16 of the design plan).

## Gamma REST API — `GET /markets`

**Base URL:** `https://gamma-api.polymarket.com`
**Path:** `/markets`

Documented query parameters (from docs): `limit`, `offset`, `order` (comma-separated
field list), `ascending`, `id[]`, `slug[]`, `clob_token_ids[]`, `condition_ids[]`,
`liquidity_num_min/max`, `volume_num_min/max`, `start_date_min/max`, `end_date_min/max`,
`tag_id`, `related_tags`, `cyom`, `uma_resolution_status`, `game_id`,
`sports_market_types[]`, `rewards_min_size`, `question_ids[]`, `include_tag`,
`closed` (default `false`).

Request used for the live capture:
```
GET https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=3&order=volume24hr&ascending=false
```
Result: HTTP 200, 3 markets returned. Saved verbatim in
`internal/polymarket/testdata/gamma-markets.sample.json`.

**Real response field list observed** (superset of what the docs list; the docs
snippet mentions the core subset — `id`, `question`, `slug`, `conditionId`,
`clobTokenIds`, `outcomes`, `outcomePrices`, `volume`, `volumeNum`, `endDate`, `active`,
`closed`, plus pricing/liquidity/tag fields):

```
id, question, conditionId, slug, resolutionSource, endDate, liquidity, startDate,
image, icon, description, outcomes, outcomePrices, volume, active, closed,
marketMakerAddress, createdAt, updatedAt, new, featured, archived, resolvedBy,
restricted, groupItemThreshold, questionID, enableOrderBook, orderPriceMinTickSize,
orderMinSize, umaResolutionStatus, volumeNum, liquidityNum, endDateIso, startDateIso,
hasReviewedDates, volume24hr, volume1wk, volume1mo, volume1yr, gameStartTime,
secondsDelay, clobTokenIds, positionIds, comboStatus, umaBond, umaReward,
volume24hrClob, volume1wkClob, volume1moClob, volume1yrClob, volumeClob,
liquidityClob, makerBaseFee, takerBaseFee, customLiveness, acceptingOrders, negRisk,
events, ready, funded, acceptingOrdersTimestamp, cyom, competitive,
pagerDutyNotificationEnabled, approved, rewardsMinSize, rewardsMaxSpread, spread,
oneDayPriceChange, oneHourPriceChange, lastTradePrice, bestBid, bestAsk,
automaticallyActive, clearBookOnStart, manualActivation, negRiskOther,
sportsMarketType, umaResolutionStatuses, pendingDeployment, deploying,
deployingTimestamp, rfqEnabled, holdingRewardsEnabled, feesEnabled,
requiresTranslation, feeType, marketMetadata, feeSchedule, version
```

Important, verified from the real payload:
- `clobTokenIds` and `outcomes` and `outcomePrices` are **JSON-encoded strings**, not
  arrays — e.g. `"clobTokenIds": "[\"753138...\", \"453533...\"]"`. Must
  `json.Unmarshal` twice (outer object, then the string field).
- There is **no plain `category` or `tags` array** on the market object itself in this
  response; category/topic info lives under `events`/`marketMetadata` (not explored
  further in Phase 0 — the design plan's `internal/polymarket/gamma` package should
  confirm the exact shape in Phase 1).
- `volumeNum` is a JSON number; `volume`, `volume24hr` etc. are decimal strings.

## CLOB WebSocket — market channel

**Endpoint (from docs):** `wss://ws-subscriptions-clob.polymarket.com/ws/market`

**Subscribe (from docs):**
```json
{ "assets_ids": ["<token_id>", "..."], "type": "market" }
```
Optional flag to enable extra event types (`best_bid_ask`, `new_market`,
`market_resolved` — from docs, not verified live):
```json
{ "assets_ids": ["<token_id>"], "type": "market", "custom_feature_enabled": true }
```
Dynamic subscribe/unsubscribe after the connection is open (from docs):
```json
{ "assets_ids": ["<token_id>"], "operation": "subscribe" }
{ "assets_ids": ["<token_id>"], "operation": "unsubscribe" }
```

**Heartbeat (from docs, confirmed live):** the client must send the *text frame*
`PING` (not a JSON message, not a WS protocol ping) periodically; the docs say every
10 s. The probe script sent `PING` every 10 s and received a `PONG` text frame back —
confirmed live (`probe-summary.json`: `"pong_seen": true`).

**Per-connection asset limit:** not stated anywhere in the docs found. Design plan
§4.1's "~200 assets per shard" is our own conservative choice, not a documented limit.

### Event types — captured live vs. docs-only

Live capture: connected, subscribed to 6 asset IDs from the 3 highest-24h-volume
Gamma markets, and recorded frames for ~20s (capped at 60 saved frames). Result
(`probe-summary.json`):
```json
{
  "event_types": { "book": 26, "price_change": 29, "last_trade_price": 10 },
  "pong_seen": true,
  "frames_saved": 60
}
```
Raw frames, one per line, exactly as received, are in
`internal/polymarket/testdata/clobws-market.sample.ndjson`.

| Event type | Status | Where |
|---|---|---|
| `book` | **captured live** | `clobws-market.sample.ndjson` |
| `price_change` | **captured live** | `clobws-market.sample.ndjson` |
| `last_trade_price` | **captured live** | `clobws-market.sample.ndjson` |
| `tick_size_change` | docs only (not seen in the ~20s window) | `clobws-events.from-docs.json` |
| `best_bid_ask` | docs only (needs `custom_feature_enabled`, not tried) | `clobws-events.from-docs.json` |
| `new_market` | docs only (needs `custom_feature_enabled`, rare event) | `clobws-events.from-docs.json` |
| `market_resolved` | docs only (needs `custom_feature_enabled`, rare event) | `clobws-events.from-docs.json` |

Observed live field lists (matches the docs' examples field-for-field):
- `book`: `market`, `asset_id`, `bids[]{price,size}`, `asks[]{price,size}`, `hash`, `timestamp`. Note: `event_type` was present on some frames but the very first captured frame was a **JSON array of 6 book objects** sent as the initial snapshot right after subscribing — one element per subscribed asset, each without a top-level `event_type` wrapper on the array itself (it's per-element). Implementers should be prepared to receive either a single object or an array of objects on one text frame.
- `price_change`: `market`, `price_changes[]{asset_id,price,size,side,hash,best_bid,best_ask}`, `timestamp`.
- `last_trade_price`: `market`, `asset_id`, `price`, `size`, `fee_rate_bps`, `side`, `timestamp`, `event_type`, `transaction_hash`.

All price/size fields are **strings**, not JSON numbers, in every live frame observed.
`timestamp` is a millisecond epoch encoded as a string.

## Reproduction

```sh
node scripts/probe-polymarket.mjs [outDir] [captureSeconds]
```
Requires Node with global `fetch`/`WebSocket` (Node 22+; this repo uses Node 24). No
dependencies. Re-running overwrites the sample files with a fresh capture.
