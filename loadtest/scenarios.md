# Load test scenarios — methodology

This documents exactly how S1-S6 (design-plan.md section 9) were run for real
against the docker-compose stack on the machine described in
`docs/benchmark.md`. Numbers are in `docs/benchmark.md`; this file is the
"how", not the "what we found".

## Common setup

```
cd deploy
docker compose -f docker-compose.yml up -d --build
# redpanda, redpanda-init, ingestor, processor, gateway-1, gateway-2, recorder
```

No `docker-compose.prod.yml` resource limits are applied - the whole point of
S1/S2 is to find *this machine's* real ceiling, and the prod override's
CPU/memory caps are sized for the design's 2 vCPU/4 GB VPS target, not this
16-thread/24 GB dev machine. Grafana/Prometheus were not brought up for the
benchmark runs themselves: metrics were read directly from each service's
`/metrics` (host ports 9091-9094) with `curl`, or from
`docker exec oddspulse-redpanda-1 rpk group describe <group>` for
authoritative consumer-group lag (see the S1 note below on why the
processor's own `processor_consume_lag` gauge is not trustworthy under
overload). This is a fully headless CLI environment, so no Grafana
screenshots are included; the dashboard JSON itself was already validated in
Phase 5.

Tools used:
- `cmd/loadgen-producer` (new this phase): synthetic per-asset random-walk
  generator, `-rate`, `-assets`, `-workers`, `-surge-prob`, and (new)
  `-real-assets-url` to target real, client-visible asset ids instead of its
  own synthetic ones.
- `cmd/loadgen-clients` (extended this phase): `-conns`, `-subs-per-conn`,
  plus new `-hot-asset` (S3) and `-slow-frac` (S4) flags.
- Both are run two ways: natively on the Windows host (`bin/*.exe`, talking
  to `localhost:19092` / `localhost:808x`), and packaged into a throwaway
  `oddspulse-loadgen-clients` Docker image run with
  `--network oddspulse_default` so it can dial gateway containers directly
  by service name (`ws://gateway-1:8080/ws`). The in-network path matters:
  see S2.

## S1 — Ingest ceiling

```
bin/loadgen-producer.exe -brokers localhost:19092 -rate <RATE> -assets <N> -workers <W> -duration 45-60s
```

Ramped `<RATE>` = 10,000 / 25,000 / 50,000 / 60,000 / 75,000 eps (assets
scaled roughly with rate: 2,000 -> 5,000 synthetic assets, `-workers` 4-10).
Real Polymarket traffic kept flowing through the ingestor throughout (mixed
into the same `pm.raw` topic, negligible volume next to the synthetic rate).

Every 15s during each run: sampled `processor_events_total`,
`gateway_e2e_latency_seconds_{sum,count}` (from gateway-1, since e2e latency
is measured where the tick is consumed off `pm.ticks`), and
`docker stats --no-stream` for processor/gateway/redpanda CPU/mem. Mean
per-interval latency is `Δsum/Δcount`.

**Important caveat discovered live:** `processor_consume_lag` (the
processor's own gauge) reads 0 throughout every run, including ones that are
provably melting down (see results). Reading `processor/state.go`'s
`reportLag`: it `.Set()`s a value computed only from the partitions that
happened to have records in *that specific* `PollFetches` batch, so it can
never reflect true accumulated backlog across all 12 partitions - a real
observability gap, not a sign of health. `rpk group describe <group>`'s
`TOTAL-LAG` (used in S5) is the trustworthy number; the honest ceiling
signal in S1 is the `gateway_e2e_latency_seconds` trend, not the lag gauge.

## S2 — Fan-out ceiling

First attempt used `bin/loadgen-clients.exe` from the host against
`ws://localhost:8081/ws` (gateway-1's published port). At just 1,000-5,000
attempted connections, a large and *growing* fraction were refused
("connection actively refused") despite the gateway's own CPU staying low
(record kept: `loadtest/results/s2-1000.log`, `s2-5000.log`). This is not
ephemeral-port exhaustion (only ~1,000-5,000 of the 16,384 available ports
were ever in use) - it is Docker Desktop for Windows' host port-forwarding
layer failing to accept a burst of thousands of near-simultaneous new TCP
connections. Confirmed directly: the same `-conns 5000` run against
`ws://gateway-1:8080/ws` from a container on the compose network
(`oddspulse-loadgen-clients` image, `--network oddspulse_default`, no host
port involved) established 5000/5000 (100%) instantly. All further S2-S6
numbers use this in-network path, which measures the gateway's own real
capacity instead of this host's NAT layer.

```
docker run --rm --network oddspulse_default oddspulse-loadgen-clients \
  -url ws://gateway-1:8080/ws -conns <N> -subs-per-conn 20 -duration <D>s
```

Ramped `<N>` = 1,000 / 5,000 (host-mapped, for the NAT-ceiling finding above)
then 10,000 / 20,000 / 30,000 (in-network, for the real ceiling). Sampled
every 10s: `gateway_clients`, `gateway_messages_out_total`,
`gateway_bytes_out_total`, `gateway_slow_client_evictions_total`,
`go_memstats_heap_alloc_bytes` (from `/metrics`), and `docker stats` CPU/mem.
Memory-per-connection uses `go_memstats_heap_alloc_bytes` deltas (a Go
runtime counter, byte-accurate), not `docker stats` MemUsage - the latter
swung by hundreds of MB between consecutive `--no-stream` samples on this
Windows/WSL2 setup even at rest, and is documented in `docs/benchmark.md` as
unreliable here.

## S3 — Hot market

Same in-network client, `-hot-asset` (every connection subscribes to the
same single real asset id instead of its own slice) at the same connection
count as an S2 data point, for comparison:

```
docker run --rm --network oddspulse_default oddspulse-loadgen-clients \
  -url ws://gateway-1:8080/ws -conns 10000 -subs-per-conn 1 -duration 30s -hot-asset
```

**Caveat:** this is not a controlled experiment - real Polymarket's tick rate
for any single asset is low (the shared hot asset ticked about twice across
the whole 30s run), so total message volume is far lower than the S2
baseline's 20-different-assets-per-client case. What it does show cleanly:
concentrating 10,000 subscribers onto one key produces *no* CPU/memory spike
and *no* evictions relative to the spread-out baseline - the conflation
design (at most one merged frame per client per flush interval, regardless
of how many subscribed assets - or how many subscribers on one asset -
changed that interval) is doing its job. Proving the point under a
controlled, high, single-asset tick rate would require faking ticks for one
real, client-visible asset id at a chosen rate; not done this phase (noted
as a gap in `docs/benchmark.md`).

## S4 — Slow clients

```
bin/loadgen-producer.exe -real-assets-url http://localhost:8081/markets -assets 200 -rate 4000 -workers 4 -duration 75s &
docker run --rm --network oddspulse_default oddspulse-loadgen-clients \
  -url ws://gateway-1:8080/ws -conns 2000 -subs-per-conn 20 -duration 60s -slow-frac 0.1
```

`-real-assets-url` (new loadgen-producer flag, this phase) is required here:
real Polymarket's tick rate is too low to ever fill a slow client's 64-frame
outbound queue in a reasonable test window regardless of how slow the
"slow" client is, so real, client-visible asset ids are fed synthetic ticks
at a controllable rate instead - the only way to make this scenario
reproducible.

**First attempt was wrong and is worth recording:** the initial `-slow-frac`
implementation made a "slow" connection never call `Read` again at all.
That also stops it from servicing the gateway's transport-level WS ping
(coder/websocket only processes control frames while something is calling
`Read`), so all slow connections were disconnected by the 60s ping/pong
timeout, not by the queue-full backpressure eviction S4 is supposed to
exercise. Fixed (`cmd/loadgen-clients/main.go`'s `runSlowConn`): a slow
connection keeps calling `Read` (servicing pings) but only once every 2s -
far slower than the gateway's 100ms flush cycle - so it is genuinely falling
behind, not just silent.

## S5 — Chaos

```
docker kill -s SIGKILL oddspulse-processor-1   # then, separately: gateway-2-1, then ingestor-1
```

Load running during the processor kill: `loadgen-producer -rate 10000
-assets 2000 -workers 4 -duration 60s`. Load running during the gateway-2
kill: 500 in-network clients against gateway-1 (the *other*, untouched
instance) for the duration, to directly observe whether it stays unaffected.
No extra load during the ingestor kill - the thing being measured there is
real-WS reconnect time.

Recovery tracked via: container `docker inspect .State.Status`,
`docker exec oddspulse-redpanda-1 rpk group describe <group>`'s TOTAL-LAG
(authoritative backlog, see the S1 note), and each service's own
`/metrics`/`/readyz`. **Every one of the three kills required a manual
`docker compose up -d <service>`** - `restart: unless-stopped` did not
automatically restart any of the three killed containers within the observation
window (worst case: processor, left unattended for ~90s). This is reported
as its own finding in `docs/benchmark.md`, separate from the recovery-time
numbers (which are measured from the manual restart, the real moment
recovery actually began).

## S6 — Horizontal scale

Same 20,000-total-client load, run two ways back to back for direct
comparison: all 20,000 against gateway-1 alone, vs 10,000 against gateway-1
simultaneously with 10,000 against gateway-2 (two separate
`docker run ... oddspulse-loadgen-clients` invocations in parallel, same
`-conns 10000 -subs-per-conn 20 -duration 35s` each).

## Optimizations (design-plan.md section 9's list)

All four run the same way: a *temporary*, isolated third gateway container
(`docker run -d --rm --name gw-opt-<label> --network oddspulse_default ...
oddspulse-gateway-1 <env overrides>`, host ports 8083/9096) so gateway-1/
gateway-2's own S1-S6 numbers are never disturbed. Fixed load for every
variant: 2,000 in-network clients, 20 subs/client, 30s, against a background
`loadgen-producer -real-assets-url ... -rate 3000 -assets 200` feed so every
variant sees comparable volume.

- **Pre-encoding vs naive re-marshal**: `GW_NAIVE_REMARSHAL=true` (new env
  var, `internal/hub`'s `Config.NaiveRemarshal` - a measurement-harness-only
  toggle added this phase, decodes+re-encodes each tick once per subscribed
  client instead of sharing the one pre-encoded byte slice) vs the default
  `false`.
- **Batching interval**: `GW_FLUSH_MS` = 0, 50, 100 (default), 250.
  `GW_FLUSH_MS=0` originally crashed the gateway outright
  (`time.NewTicker(0)` panics) - fixed in `internal/hub/hub.go` (floors to
  1ms) since the design plan explicitly lists 0ms as one of the intervals to
  benchmark.
- **GOGC/GOMEMLIMIT**: default (`GOGC=100`, no limit) vs `GOGC=50
  GOMEMLIMIT=256MiB`.
- **Sharded vs global mutex**: no build tag or config knob exists for
  `internal/hub/index.go`'s shard count (`numShards = 64` is a compile-time
  constant) - retrofitting one purely to run this one comparison, then
  deleting it, was judged not worth doing. Reasoned instead from the S2
  20,000-client CPU profile (`loadtest/profiles/gateway-cpu-s2-20k.pb.gz`):
  no `internal/hub` index/shard-lock function appears anywhere in it, at any
  percentage - see `docs/benchmark.md`.
- **JSON vs binary encoding**: skipped. Explicitly "Phase 6 optional" in
  design-plan.md section 5's Go model note; building a second wire encoding
  just to fill this checkbox was judged not a good use of this phase's time.

## pprof

```
go tool pprof -top -nodecount=15 http://localhost:9093/debug/pprof/heap        # idle, right after a gateway-1 restart
go tool pprof -seconds 25 -top -nodecount=25 http://localhost:9093/debug/pprof/profile   # CPU, during a 20,000-client in-network run
```

`net/http/pprof` is registered on the gateway's existing metrics listener
(`cmd/gateway/main.go`), left unconditionally on rather than gated behind a
flag: that port is never exposed beyond the same docker-compose host-port
mapping `/metrics` already uses, and there is no public-facing deployment of
this service yet.

## Cleanup

`loadtest/results/*.log` (raw scrape/docker-stats output, one file per
step above) are gitignored - they are reproducible by re-running the exact
commands above, and several of the S1/S2 ones are large. `docs/benchmark.md`
has the summarized tables. `loadtest/profiles/` keeps two representative
pprof snapshots (one heap, one CPU) as evidence, not every profile taken.
