# Architecture Decision Records

Short, one-page records of decisions with lasting consequences. See the design
plan §11 for the full planned list.

| ADR | Title | Status |
|---|---|---|
| [001](001-redpanda-over-kafka.md) | Use Redpanda instead of Apache Kafka | Accepted |
| [002](002-gateway-broadcast-consumer-group.md) | Gateway uses one consumer group per instance (broadcast fan-out) instead of a shared consumer group | Accepted |
| [003](003-batching-conflation-interval.md) | Batch and conflate gateway → client ticks, flush interval revised to 50 ms after Phase 6's measurements | Accepted |
| [004](004-at-least-once-delivery.md) | At-least-once delivery end to end, with client-side idempotency via `seq`/timestamps | Accepted |
| [005](005-frontend-on-github-pages-with-replay-fallback.md) | Host the frontend on GitHub Pages, with a replay fallback | Accepted |
| [006](006-dependency-choices.md) | Two dependency choices formalized: `coder/websocket` over `gorilla/websocket`, `caarlos0/env` over `koanf` | Accepted |
