# Architecture Decision Records

Short, one-page records of decisions with lasting consequences. See the design
plan §11 for the full planned list.

| ADR | Title | Status |
|---|---|---|
| [001](001-redpanda-over-kafka.md) | Use Redpanda instead of Apache Kafka | Accepted |
| 002 | Gateway uses one consumer group per instance (broadcast fan-out) instead of a shared consumer group | Planned — written in Phase 3 alongside the gateway implementation |
| 003 | Batch and conflate gateway → client ticks on a 100 ms flush interval | Planned — written in Phase 3, informed by the load-test results in §9 |
| 004 | At-least-once delivery end to end, with client-side idempotency via `seq`/timestamps | Planned — written in Phase 2 alongside the processor implementation |
| [005](005-frontend-on-github-pages-with-replay-fallback.md) | Host the frontend on GitHub Pages, with a replay fallback | Accepted |
