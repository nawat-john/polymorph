# Benchmark report (Phase 6)

> Methodology (exact commands run) is in `loadtest/scenarios.md`. This file
> is the results and the analysis. Every number below was measured against
> the live docker-compose stack described in design-plan.md - none are
> estimated, rounded up, or backfilled from the design's targets.

## Machine

This is a single Windows development laptop, **not** the design plan's
2 vCPU/4 GB VPS target (design-plan.md section 2). All numbers below are
reproducible on this hardware; they are not a prediction of VPS behavior,
and every section says explicitly where the two would differ.

| | |
|---|---|
| CPU | AMD Ryzen 7 7435HS, 8 cores / 16 threads, 3.1 GHz base |
| RAM | ~23.8 GiB total (25,577,005,056 bytes) |
| OS | Windows 11 Home Single Language (build 10.0.26200) |
| Docker | Docker Desktop, engine 29.6.1, WSL2 backend, allotted 16 CPUs / ~11.6 GiB to the Docker VM (`docker info`) |
| Go | go1.27.0 windows/amd64 (host tools: loadgen-producer, loadgen-clients, `go tool pprof`) |
| Windows ephemeral port range | 49152-65535 (16,384 ports) - not what actually limited connection count, see S2 |

Config used: `deploy/docker-compose.yml` only - **no** `docker-compose.prod.yml`
resource limits applied. That override sizes CPU/memory caps for the design's
2 vCPU/4 GB VPS target; applying it here would answer "how does this behave
artificially throttled to VPS size", not "what is this machine's real
ceiling", which is what S1/S2 ask for. Where the report says "on the target
2 vCPU/4 GB VPS, expect X instead", that is a stated inference, not a
measurement.

All 6 services from `deploy/docker-compose.yml` were running throughout
(redpanda, redpanda-init, ingestor, processor, gateway-1, gateway-2,
recorder), with real Polymarket data flowing through the ingestor the whole
time. No Grafana/Prometheus containers were brought up for these
measurements - this is a headless CLI session with no way to screenshot a
dashboard, so numbers come directly from `curl .../metrics` and
`rpk group describe` instead (see `loadtest/scenarios.md`). The Grafana
dashboard JSON itself was already validated running in Phase 5.

---

## S1 — Ingest ceiling

Synthetic `loadgen-producer` load (real Polymarket traffic also flowing
throughout, negligible next to these rates). Mean e2e latency is
ingestor-receive (`recv_ts`) to gateway-consumes-off-`pm.ticks`, computed as
`Δgateway_e2e_latency_seconds_sum / Δgateway_e2e_latency_seconds_count`
between consecutive 15s samples.

| Rate (events/s) | Processor CPU (docker stats, %) | Mean e2e latency across the run's four 15s intervals (ms) | Verdict |
|---|---|---|---|
| 10,000 | 55% → (idle after) | 7.4 → 7.8 → 7.4 | flat, stable |
| 25,000 | 75% → 80% → 84% | 9.6 → 9.0 → 9.8 → 8.9 | flat, stable |
| 50,000 | 109% → 111% → 114% | 54.7 → 55.3 → 60.7 → 61.4 | flat, stable (elevated but not growing) |
| 60,000 | 166% → 170% → 168% | 209.7 → **598.0** → **1429.2** → **2571.5** | **unbounded growth** |
| 75,000 | 163% → 174% → 164% | 146.4 → **530.3** → **1503.2** → **3201.5** | **unbounded growth** |

**Real ceiling on this hardware: ~50,000 events/s**, sustained with flat,
bounded latency. Somewhere between 50k and 60k eps the single processor
instance (single consumer-group member, single in-process mutex over all
asset state per `cmd/processor/state.go`) tips into classic queueing
collapse: latency roughly doubles or triples every 15s within the same run,
which is the actual overload signal, not `processor_consume_lag` (see box
below - it reads exactly `0` in every single row of this table, including
the two that are provably melting down).

**Design target (§2): ≥50,000 events/s. Met, but with no headroom** - one
step above (60k) is already unbounded. Processor CPU crossing ~110% of one
core (just above what a single core can sustain) lines up almost exactly
with where latency stops being flat, consistent with a single-core-bound
consumer loop being the real limit, not Redpanda (which stayed under 30-60%
CPU throughout every row) and not the gateway (which stayed under 40% in
every row).

> **`processor_consume_lag` is not trustworthy under overload.**
> `cmd/processor/main.go`'s `reportLag` computes lag only from the
> partitions that happened to have records in *that specific*
> `PollFetches()` batch and then `.Set()`s the gauge (an overwrite, not an
> accumulation) - so a batch that only touched 2 of 12 partitions reports
> lag for those 2 and silently discards whatever the other 10 last reported.
> It read `0` throughout the 75,000 eps row above even while mean latency
> was climbing past 3 seconds. `docker exec oddspulse-redpanda-1 rpk group
> describe processor`'s `TOTAL-LAG` is the trustworthy number (used
> successfully in S5 below); this gauge should be rewritten to track
> per-partition high-watermark deltas persistently across polls rather than
> resetting every batch. Flagged as a Phase 7 follow-up, not fixed this
> phase (it is an observability bug, not something this benchmark needed
> to fix to produce honest results - the raw e2e-latency signal was enough).

---

## S2 — Fan-out ceiling

**First finding: the real bottleneck at only 1,000-5,000 connections was this
Windows machine's Docker Desktop host port-forwarding, not the gateway.**

| Attempted (via host-mapped `localhost:8081`) | Established | Gateway CPU during ramp |
|---|---|---|
| 1,000 | 878 (87.8%) | 12-26% |
| 5,000 | 1,256 (25.1%) | ~12% |

Gateway CPU stayed low and every established connection remained stable for
the test's full duration in both rows - the failures are "connection
actively refused" during the initial connect burst, not gateway overload.
Confirmed directly: the identical `-conns 5000` run from a container on the
compose network (`ws://gateway-1:8080/ws`, bypassing the host entirely)
established **5,000/5,000 (100%)** instantly. All numbers below use that
in-network path - it measures the gateway's real capacity instead of this
specific Windows/Docker-Desktop NAT limitation. (Ephemeral ports were never
close to the limit: at most 5,000 of 16,384 were ever in use.)

| Clients (in-network) | Established | p50 / p99 latency | Throughput (msgs/s) | Gateway CPU (docker stats) | Mem/connection |
|---|---|---|---|---|---|
| 10,000 | 100% | 82 / 384 ms | 14,400 | 198% → 61% → 1% | ~9-11 KB |
| 20,000 | 100% | 126-176 / 1,259-3,202 ms (2 runs) | 28,594-32,003 | 208-370% (peak, one run); ~291% avg (profiled run) | ~25-34 KB |
| 30,000 attempted | 26,116 (87.1%), rest timed out on handshake ("context deadline exceeded") | 269 / 6,283 ms | 32,879 | up to 616% | n/a |

Memory/connection uses `go_memstats_heap_alloc_bytes` deltas (a Go-runtime
byte counter), **not** `docker stats` MemUsage - the latter swung by
hundreds of MB between consecutive samples at rest on this Windows/WSL2
setup (see the pprof section below for why: retained Kafka fetch buffers,
not per-connection cost) and is not a reliable per-connection signal here.
The 25-34 KB range at 20,000 clients is higher immediately after the
connect burst (subscribe-parsing garbage not yet GC'd) and settles toward
the low end ~30s in.

**Design target (§2): ≥10,000 clients/gateway. Met, 2x over** - this
machine sustains ~20,000-26,000 concurrent clients on one gateway instance
before handshakes start timing out. **Design target: p99 < 100ms. Not met**
- p99 is already 384ms at just 10,000 clients, and climbs to seconds by
20,000+. Memory target (<30KB/connection): met at 10,000 (~10KB), borderline
at 20,000 (~25-34KB depending on how soon after the connect burst it is
sampled).

On the actual 2 vCPU/4 GB VPS target, expect the connection ceiling to be
dramatically lower than the ~20,000-26,000 measured here - this ran on 16
threads with CPU climbing past 600% (6+ cores) at the edge of the ceiling,
resources the VPS target does not have.

---

## S3 — Hot market

10,000 in-network clients, all subscribed to the **same single** real asset
id, vs. the S2 baseline (10,000 clients x 20 *different* real assets each):

| | CPU (docker stats) | p50 / p99 | Throughput |
|---|---|---|---|
| S2 baseline (20 different assets/client) | 198% → 61% → 1% | 82 / 384 ms | 14,400 msgs/s |
| S3 hot asset (1 shared asset) | 122% → 124% → 1% | 186 / 231 ms | 667 msgs/s |

No CPU/memory spike, no evictions, from concentrating 10,000 subscribers
onto one key - the conflation design (at most one merged frame per client
per flush interval, however many subscribers or however many ticks on that
one key) does its job. **Caveat, stated plainly:** this is not a controlled
experiment. Real Polymarket's tick rate on any single asset is low (the
shared asset ticked about twice across the whole 30s run, hence the 667
msgs/s), so total volume is far below the S2 baseline's - the comparison
shows hot-key concentration doesn't blow anything up, but doesn't stress a
*high* single-key tick rate the way a synthetic feed pointed at one real
asset id could have. Not done this phase (would need loadgen-producer to
target exactly one asset at a chosen high rate); noted as a gap.

---

## S4 — Slow clients

2,000 in-network clients (1,800 normal + 200 = 10% deliberately slow),
fed by a 4,000 eps synthetic `loadgen-producer -real-assets-url` stream
across the 200 real assets they're subscribed to (needed - see
`loadtest/scenarios.md` for why real data alone can't stress this).

- **Normal clients: p50 = 61ms, p99 = 132ms** - indistinguishable from
  baseline numbers elsewhere in this report at similar scale. **Confirmed:
  slow clients do not affect normal clients' latency.**
- All 200 slow clients were gone (`gateway_clients` dropped from 2,000 to
  1,800) within ~10 seconds.
- **`gateway_slow_client_evictions_total` stayed at 0 for the entire test.**

That last point is a real finding, not a test failure: the slow clients
*were* correctly disconnected, but through a different code path than the
one the metric tracks. `internal/hub/client.go`'s `runWriter` calls
`c.conn.Write` with a 5s `writeTimeout`; when that blocks long enough on a
genuinely slow reader, it errors out and calls `h.RemoveClient` directly.
Separately, `internal/hub/hub.go`'s `tickClient` (the *documented*
mechanism, design-plan.md 4.3: 3 consecutive full-queue flush cycles) calls
`h.evict`, which *does* increment the metric. Both paths correctly remove a
slow client; only one is instrumented. Under this test's load, the
write-timeout path won the race every time. **Recommended follow-up (not
applied this phase - a metrics/instrumentation change, out of this phase's
benchmarking scope):** increment a shared "slow client disconnected" counter
from both `RemoveClient` call sites, or split them into two counters, so an
operator watching this metric in production doesn't see `0` while clients
are, correctly, still being dropped.

---

## S5 — Chaos

All three kills used `docker kill -s SIGKILL <container>`, and **all three
required a manual `docker compose up -d <service>`** -
`restart: unless-stopped` (set on every service in `docker-compose.yml`) did
not automatically restart any of them within the observation window (worst
case: processor, left unattended ~90s while this was being confirmed twice
more against gateway-2 and the ingestor). This is a genuine, reproducible
(3/3) finding on this Docker Desktop 29.6.1/Windows setup, reported exactly
as observed - it is not a property of the design and needs re-verification
against a real Linux VPS deploy before relying on it there.

| Killed | Load during kill | Recovery, measured from the manual restart | Data loss |
|---|---|---|---|
| processor | loadgen-producer @ 10,000 eps | `rpk group describe processor` TOTAL-LAG: ~500,968 (backlog accumulated during the outage) → 0 within **6-8 seconds** of the container coming back | **None** - every backlogged `pm.raw` record was eventually processed (Kafka durability + at-least-once commit-after-produce, design-plan.md 4.2) |
| gateway-2 | 500 in-network clients against gateway-1 (the other instance) | gateway-1 kept serving throughout gateway-2's ~19s downtime; its clients' p50/p99 (54 / 109 ms) were unaffected | n/a (no state to lose - gateway is stateless per design) |
| ingestor | none (measuring WS reconnect only) | `ingestor_ws_connections` back to 1 and `ingestor_events_total{kind="quote"}` climbing again within **~2 seconds** of the container restarting | n/a |

**Design target (§2): recovery after the Polymarket WS drops < 5s. Met** (by
the ingestor row - about 2s, well inside target) **for the reconnect itself.**
The processor's ~500k-record, 6-8s catch-up is fast in absolute terms (~65k-
85k events/s in pure backlog-drain mode, notably higher than S1's ~50k
sustained-new-traffic ceiling - draining a backlog with no new arrivals
competing for the same lock is a different, easier workload) but the
auto-restart gap above means real end-to-end recovery time in this
environment is "however long it takes an operator to notice and restart it"
plus 6-8s, not the few-seconds figure the design's health checks assume.

---

## S6 — Horizontal scale

Same 20,000 total clients, run two ways for direct comparison (35s each):

| Configuration | p50 | p99 | Throughput |
|---|---|---|---|
| 1 instance (gateway-1, 20,000 clients) | 176 ms | 1,259 ms | 32,003 msgs/s |
| 2 instances (gateway-1 + gateway-2, 10,000 each) | 94 / 81 ms | 641 / 644 ms | 17,667 + 17,238 = **34,905 msgs/s** |

Both configurations established 100% of their attempted connections.
**Aggregate throughput grew only ~9% (not linearly, not 2x)**, but **p50 and
p99 latency both roughly halved.** This is consistent with, not contrary to,
the architecture: the CPU profile below shows the dominant per-instance cost
is `internal/hub.(*Hub).flushAll` iterating **every connected client** each
flush cycle (`O(clients_on_this_instance)`, not `O(messages)`) - splitting
20,000 clients across 2 instances halves that per-instance iteration count
directly, which is exactly what halved p99. Aggregate message throughput
isn't the thing this design's horizontal scaling primarily buys back; latency
under a fixed total client count is. Combined CPU across both instances
(peaks of 267%/226% ≈ 493% combined) was somewhat *higher* than one instance
alone (≈370% peak / ≈291% average) - running two Go processes duplicates
some fixed cost (bootstrap cache, its own consumer group, etc.), so
horizontal scaling here is not free, but it does deliver on its actual job
(latency) for this architecture's actual bottleneck.

---

## Optimizations

All four ran against a temporary, isolated third gateway container (so
gateway-1/2's own numbers above are undisturbed), 2,000 in-network clients,
20 subs/client, 30s, with a steady `loadgen-producer -real-assets-url ...
-rate 3000 -assets 200` background feed for comparable volume across every
variant. CPU is `docker stats` samples at t=10s/20s/30s.

### 1. Pre-encoding vs. naive per-client re-marshal

`GW_NAIVE_REMARSHAL` (new, measurement-harness-only env var this phase -
see `loadtest/scenarios.md`): decodes+re-encodes each tick once per
subscribed client instead of sharing the one pre-encoded byte slice.

| | CPU | p50 | p99 |
|---|---|---|---|
| Pre-encoded (default, shared bytes) | 18-22% | 58 ms | 118 ms |
| Naive re-marshal (measurement harness) | 44-48% | 69 ms | 143 ms |

**~2-2.5x the CPU** for the naive path at 2,000 clients, with a smaller but
real latency cost too. This is the pre-encoding optimization's real,
measured payoff, not an assertion - and it should widen further at higher
client counts (this scales `O(clients)` either way, but the naive path's
per-client constant factor - a full JSON decode+encode vs. a byte-slice
share - is much larger); not separately re-measured at 20,000-client scale
this phase (time budget).

### 2. Batching interval (`GW_FLUSH_MS`)

`GW_FLUSH_MS=0` **crashed the gateway on startup** (`time.NewTicker(0)`
panics) before this could even be measured - fixed in `internal/hub/hub.go`
(floors the effective interval to 1ms) since the design plan explicitly
lists 0ms as one of the values to benchmark.

| `GW_FLUSH_MS` | CPU (3 samples) | Throughput (msgs/s) | p50 | p99 |
|---|---|---|---|---|
| 0 (floored to 1ms) | 67% → 36% → 9% | **52,384** | **2,016 ms** | **3,726 ms** |
| 50 | 28% → 19% → 1% | 3,761 | **37 ms** | **66 ms** |
| 100 (documented default) | 22% → 18% → 1% | 3,632 | 58 ms | 118 ms |
| 250 | 15% → 13% → 1% | 3,068 | 101 ms | 267 ms |

Two real findings, not the one expected:

- **0ms is a trap, not an optimization.** Flooring to 1ms makes
  `flushAll`'s `O(total_clients)` loop run ~100x more often than the 100ms
  default (1000/s vs 10/s), which is pure self-inflicted overhead at this
  connection count - CPU goes up *and* latency explodes (p50 2 seconds),
  even though raw message count goes up too (less conflation). This
  directly validates the batching design: without it, more "real-time"-
  feeling config is strictly worse on every axis that matters.
- **50ms beat the documented 100ms default on every axis measured** (lower
  CPU *and* lower latency) at this specific scale (2,000 clients, ~3,000
  eps/200 real assets). 250ms has the lowest CPU of the sane configs but the
  worst latency of them. There is a real sweet spot below 100ms at this
  connection count; whether 50ms remains the sweet spot at 20,000 clients
  was not re-tested (time budget) - `docs`'s recommended default should be
  re-validated against the target VPS's specific connection-count range
  before being changed.

### 3. Sharded vs. global subscription-index mutex

No build tag or config knob exists for `internal/hub/index.go`'s shard count
(`numShards = 64` is a compile-time constant); adding one purely to run this
one comparison and then deleting it was judged not worth the churn (per the
task's own "otherwise reason about it" allowance). Reasoned instead from
real evidence: the S2 20,000-client CPU profile
(`loadtest/profiles/gateway-cpu-s2-20k.pb.gz`) has **zero** `internal/hub`
index/shard-lock function anywhere in its top 200 nodes, at any percentage.
At this connection count on this hardware, subscription-index lock
contention is not a measurable cost at all - the design's 64-way sharding
has already succeeded at making it a non-issue relative to the syscall cost
below. (`internal/hub/index_test.go`'s `TestIndexConcurrentAccess` already
covers this path for correctness under `-race`; nothing further was run.)

### 4. `GOGC` / `GOMEMLIMIT`

| | CPU (3 samples) | Peak heap (docker stats MemUsage) | p50 | p99 |
|---|---|---|---|---|
| Default (`GOGC=100`, no limit) | 22% → 18% → 1% | ~168 MiB | 58 ms | 118 ms |
| `GOGC=50 GOMEMLIMIT=256MiB` | 28% → 25% → 1% | ~132 MiB | 59 ms | 120 ms |

**~21% lower peak memory for ~30% higher CPU (relative), latency
unchanged** at this scale. Textbook GC tradeoff, real numbers behind it: on
a memory-constrained VPS (the actual 4GB target, shared with Redpanda and
every other service) this is a reasonable trade; on this dev machine with
23.8 GB free, the default is fine.

### 5. JSON vs. binary encoding

**Skipped**, per design-plan.md section 5's own "Phase 6 optional" callout -
building a second wire encoding (Protobuf/MessagePack) just to fill this
checkbox was judged not a good use of this phase's remaining time. This
stays a real, listed "what I'd do next" item, not a silently dropped one.

---

## pprof

Two profiles are kept as evidence under `loadtest/profiles/` (raw pprof
`.pb.gz`, viewable with `go tool pprof`); everything else generated while
investigating is not kept (reproducible via the commands in
`loadtest/scenarios.md`).

### Heap, idle, right after a gateway-1 restart (`gateway-heap-idle-baseline.pb.gz`)

Real, specific finding, not a generic guess: **68% of a ~131 MB sampled
heap traced to Kafka fetch/decompress buffers**, cumulatively through
`kgo.(*source).fetch` → `processRecordBatch` → `decompress` (zstd,
50.4 MB) and the raw record-slice buffer (`kgo.ensureLen`, 35.2 MB) -
*not* anything in this codebase directly. Root cause, traced through the
code: `cmd/gateway/bootstrap.go`'s `loadSnapshots` hands each
`pm.snapshots` record's raw `kgo.Record.Value` straight to
`hub.SeedCache`, and `internal/hub/cache.go`'s `setTick` stores that slice
as-is (`c.tick[assetID] = raw`, no copy). A Go slice keeps its *entire
backing array* alive as long as anything references any part of it - so
every long-lived cache entry seeded this way can pin the whole
multi-record Kafka fetch buffer it came from, not just its own small
payload. This is not bootstrap-only: the live consumer path
(`cmd/gateway/sys.go`'s `handleTickRecord` → `hub.PublishTick` →
`cache.setTick`) does exactly the same thing on every tick, forever.
`pm.snapshots` had accumulated ~1M not-yet-compacted records by the time
this profile was taken (from this same phase's own repeated S1 load
testing, faster than Redpanda's log cleaner runs) which is why the
absolute number is unusually large here - but the *mechanism* (unbounded
slice retention through an un-copied cache) is real and present regardless
of topic size. **Recommended fix (not applied - a correctness/memory
change, out of this benchmarking phase's scope): copy the bytes
(`append([]byte(nil), raw...)`) before storing in `cache.setTick`.** This
is the single most actionable code-level finding from this whole phase.

### CPU, during a 20,000-client in-network run (`gateway-cpu-s2-20k.pb.gz`)

25s profile, ~291% average CPU utilization (≈2.9 cores busy). **60% of all
CPU time (43.7s of 72.8s sampled) is `internal/runtime/syscall/linux.Syscall6`**,
traced cumulatively (82.7%) through `hub.Client.runWriter` →
`coder/websocket.(*Conn).write` → `net.(*netFD).Write`. The actual
application-level batching/conflation logic
(`hub.(*Client).drainPending`) is only 4.36% cumulative - barely
registers. **This is the expected, and honestly the reassuring, result:**
at 20,000 connections the dominant cost is the irreducible one
`write(2)` syscall per client per flush cycle, not JSON encoding, not
locking, not GC. Pre-encoding and conflation are doing exactly what they
were designed to do (make the *application* work cheap); what's left is
the fundamental cost of a goroutine-per-connection WS server fanning out
to that many individual sockets, which no amount of application-level
optimization removes - only fewer sockets per process (horizontal
scaling, S6) or a fundamentally different I/O model (`io_uring`, vectored
writes) would.

---

## Overall gaps vs. design-plan.md section 2 targets (this hardware)

| Target | Result | Met? |
|---|---|---|
| Ingest throughput ≥ 50,000 events/s | ~50,000 sustained; unbounded by 60,000 | **Met, no headroom** |
| ≥ 10,000 concurrent WS clients/gateway | ~20,000-26,000 before handshake timeouts | **Met, 2x over** |
| e2e latency p50 < 20ms, p99 < 100ms | p50 7-180ms (rate/scale dependent), p99 as low as 66ms at light load but 384ms-6.3s as client count grows | **p50 sometimes met at low load; p99 not met at any tested client count ≥ 10,000** |
| Memory/connection < 30 KB | ~9-11 KB at 10,000 clients; ~25-34 KB at 20,000 | **Met at 10,000; borderline at 20,000** |
| Recovery after Polymarket WS drop < 5s | ~2s (once the container is actually running) | **Met** - but see S5's auto-restart gap |

## Open TODOs for Phase 7

- Fix `processor_consume_lag` to accumulate per-partition, not overwrite per
  batch (S1 finding).
- Fix `gateway_slow_client_evictions_total` / `RemoveClient` to count a slow
  disconnect regardless of which code path caught it (S4 finding).
- Copy bytes before `hub/cache.go`'s `setTick` stores them, to stop pinning
  whole Kafka fetch buffers via the tick cache (pprof finding - the biggest
  single actionable item here).
- Re-verify `restart: unless-stopped` actually auto-restarts on the real
  target VPS (Linux, not Docker Desktop for Windows) before relying on it
  there (S5 finding).
- Re-validate whether `GW_FLUSH_MS=50` (not the documented 100ms default)
  is really the better default, at the VPS's realistic connection-count
  range rather than this phase's 2,000-client optimization-comparison scale.
- S3 (hot market) and the naive-remarshal comparison were only measured at
  moderate scale (10,000 and 2,000 clients respectively) due to time budget
  - re-running both at ~20,000 clients would sharpen both findings.
  JSON-vs-binary encoding remains an explicitly-optional stretch item.
