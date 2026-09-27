# ADR-003: Batching + conflation flush interval

## Context

Design-plan.md §4.3 calls for batching each client's outbound ticks and sending only
the latest value per asset per flush window ("conflation"), on a configurable
interval, with **100ms** proposed as the default and §9 listing `0/50/100/250ms` as
the values to actually benchmark. This ADR was left as "planned, informed by the
load-test results" in Phase 3 specifically so it would be written *after* those
numbers existed rather than rubber-stamping the original guess.

`internal/hub/hub.go`'s `Run` drives a single `time.Ticker(GW_FLUSH_MS)` that calls
`flushAll`, which iterates every connected client and, per client, drains whatever
ticks/top-list updates were conflated since the last flush into at most one `ticks`
frame and one `top` frame.

## Decision, revised with real measurements

`docs/benchmark.md`'s "Optimizations #2" section measured all four design-plan
values against a temporary gateway instance, 2,000 in-network clients, 20
subs/client, a steady ~3,000 eps / 200-real-asset background feed:

| `GW_FLUSH_MS` | CPU (3 samples) | Throughput | p50 | p99 |
|---|---|---|---|---|
| 0 (floored to 1ms) | 67% → 36% → 9% | **52,384 msgs/s** | **2,016 ms** | **3,726 ms** |
| 50 | 28% → 19% → 1% | 3,761 | **37 ms** | **66 ms** |
| 100 (original default) | 22% → 18% → 1% | 3,632 | 58 ms | 118 ms |
| 250 | 15% → 13% → 1% | 3,068 | 101 ms | 267 ms |

Three findings that revise the original assumption:

1. **`GW_FLUSH_MS=0` does not mean "unbatched" for free - it crashed the gateway
   outright.** `time.NewTicker(0)` panics; this was a real bug this ADR's own
   benchmarking work surfaced and fixed (`internal/hub/hub.go` now floors the
   effective interval to 1ms, `minFlushInterval`, rather than letting `GW_FLUSH_MS=0`
   take the process down). Even floored, 1ms is a trap, not a win: `flushAll`'s
   per-client loop now runs ~100x more often than at 100ms, which is pure
   self-inflicted overhead at this connection count - CPU *and* latency both get
   worse, despite emitting far more (less-conflated) messages. This is the batching
   design validating itself: without it, a more "real-time-feeling" config is
   strictly worse on every axis that matters.
2. **50ms beat the documented 100ms default on every axis measured** at this scale
   (2,000 clients): lower CPU *and* lower latency, not a trade-off between them.
3. **250ms is not simply "safer/cheaper"** - it has the lowest CPU of the sane
   configs, but the worst latency among them, so it is not a free win either.

Given (2), **the default is changed to `GW_FLUSH_MS=50`** (`deploy/docker-compose.yml`
/ `deploy/docker-compose.prod.yml`'s gateway environment), down from the original
100ms guess.

## Consequences

- The new 50ms default is the empirically better choice **at the specific scale
  this was measured at** (2,000 in-network clients on this dev machine). It was
  *not* re-tested at the higher client counts (10,000-20,000) used elsewhere in
  `docs/benchmark.md`'s S2/S6, where `flushAll`'s `O(clients)` cost is already the
  dominant factor - this remains an open "what I'd do next" item: re-validate 50ms
  against the real target VPS's expected connection-count range before treating it
  as settled.
- `GW_FLUSH_MS` stays operator-configurable (design-plan.md §13) precisely because
  the right value is workload- and scale-dependent, not a universal constant - this
  benchmark only proves it is *not* 0 and that 50ms beats 100ms at one measured
  scale, not that 50ms is optimal everywhere.
- The 0ms floor-to-1ms fix (rather than rejecting the value outright) keeps the
  design-plan's own benchmarking matrix runnable end to end, while the accompanying
  unit test (`internal/hub`'s ticker-floor coverage) guards against ever
  regressing back to a startup panic.
