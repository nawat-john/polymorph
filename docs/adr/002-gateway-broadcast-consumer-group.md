# ADR-002: Gateway uses one consumer group per instance (broadcast) instead of a shared consumer group

## Context

The gateway (§4.3) fans out `pm.ticks`/`pm.alerts`/`pm.top` to whichever WebSocket
clients happen to be connected to *that specific instance*. A client connected to
`gateway-1` needs to see every tick for the assets it subscribed to, regardless of
which Kafka partition that asset's key happens to hash to - the same is true for
`gateway-2`, `gateway-3`, etc. simultaneously. This is fundamentally different from
the processor's consumption pattern (ADR-004, §4.2), where each partition's records
should be handled by exactly one instance so per-asset state isn't duplicated or
split.

A normal Kafka consumer group does load-balanced fan-out: partitions are divided
among the group's members, so each record is delivered to exactly one member. That
is the wrong semantic here - it would mean any given gateway instance, and therefore
any given client connected to it, only sees a fraction of the assets.

## Decision

Each gateway instance joins its **own, unique consumer group** at startup:

```go
// cmd/gateway/main.go
groupID := fmt.Sprintf("gateway-%s-%d", serverName(), rand.Uint64())
consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, groupID,
    []string{topicTicks, topicAlerts, topicTop},
    kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
)
```

Because Kafka guarantees every record is delivered to every distinct consumer
*group*, and this group has exactly one member, every gateway instance receives
every partition of `pm.ticks`/`pm.alerts`/`pm.top` - a broadcast, not a
load-balanced split. Fan-out to the actual WebSocket clients is then done entirely
in-process by `internal/hub`'s subscription index (§4.3), which is what decides
which of *this instance's* connected clients actually get each tick.

Offsets are never committed for this group (there is nothing to resume: a fresh
instance starts from `AtEnd()` and rebuilds any needed state from the compacted
`pm.snapshots`/`pm.markets` topics at bootstrap, see `cmd/gateway/bootstrap.go`), and
a restarted instance gets a brand new random group ID rather than reusing its old
one, so a dead instance's committed position (there is none) never affects a new
one.

## Consequences

- Adding a second/third gateway instance is pure horizontal scaling for client
  capacity and CPU (see `docs/benchmark.md` S6): each new instance independently
  sees the full stream and serves its own share of clients, with no coordination
  or repartitioning needed between instances.
- Every gateway instance pays the *full* consume cost of every topic, not a
  1/N share - N gateway instances means N times the Kafka fetch/decompress work
  for the same topics, which is the direct trade-off for statelessness and simple
  scaling. At this project's scale (a handful of instances, small compacted/short-
  retention topics) this is cheap; it would not be the right pattern for a design
  that needed to scale to dozens of gateway instances.
- Because state lives in the compacted `pm.snapshots`/`pm.markets` topics and not
  in any committed consumer offset, a gateway instance can be killed and replaced
  at any time with zero coordination with the others (verified live in
  `docs/benchmark.md` S5's gateway-2 kill: gateway-1 kept serving unaffected
  throughout).
- This only works because the gateway is (almost) stateless per design-plan.md
  §3 - it has no per-partition state that would need to survive a rebalance the
  way the processor's per-asset `AssetState` does.
