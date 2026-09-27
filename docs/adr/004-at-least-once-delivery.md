# ADR-004: At-least-once delivery, commit-after-produce

## Context

The processor (§4.2) sits between two Kafka topics: it consumes `pm.raw` and, for
each record, derives and produces zero or more `pm.ticks`/`pm.alerts`/`pm.snapshots`
records. A restart or crash can happen at any point in that pipeline, and the design
must pick, deliberately, which of the following failure modes it accepts:

- **At-most-once**: commit the `pm.raw` offset before/without confirming the derived
  produces landed - a crash between commit and produce silently loses ticks/alerts.
- **Exactly-once**: would need Kafka transactions (producer + consumer offsets in one
  transaction) - real complexity (transactional producer IDs, `read_committed`
  isolation on every consumer) for a single-broker, single-consumer-instance-per-
  partition demo pipeline where losing a tick is invisible but *fabricating* one
  is not a realistic risk here.
- **At-least-once**: commit `pm.raw` offsets only after the derived records are
  durably produced - a crash can cause the *same* batch to be reprocessed on
  restart, producing duplicate ticks/alerts/snapshots, but never silently drops one.

## Decision

Commit after produce, never before (`cmd/processor/main.go`'s `runConsumeLoop`):

```go
fetches.EachRecord(func(r *kgo.Record) { handleRecord(...) })
if err := producer.Flush(ctx); err != nil {
    produceErr = err
}
if produceErr != nil {
    // Do not commit: on restart this batch's pm.raw offsets are replayed.
    log.Warn("derived produce failed, not committing pm.raw offsets", "error", produceErr)
    continue
}
if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
    log.Warn("commit pm.raw offsets", "error", err)
}
```

`kgo.DisableAutoCommit()` is set explicitly so nothing commits behind this logic's
back. Every downstream record carries a `seq`/timestamp field
(`model.Tick.Seq`/`RecvTS`, `model.Alert.Ts`) specifically so a duplicate delivery
is detectable and safely ignorable by any consumer that cares (the gateway's
snapshot cache, for instance, just gets overwritten by the same or a newer value -
see `internal/hub/cache.go`, harmless either way after this phase's copy-on-write
fix).

## Consequences

- **No data loss** on a processor crash: `docs/benchmark.md`'s S5 chaos test killed
  the processor under a 10,000 eps load and measured `rpk group describe`'s
  `TOTAL-LAG` drain to 0 within 6-8s of restart with zero records permanently lost -
  a direct verification of this ADR's guarantee, not just a code-review claim.
- **Duplicates are possible** on the same crash window (a batch partially produced
  before the crash gets fully reprocessed on restart) - every consumer of
  `pm.ticks`/`pm.alerts`/`pm.snapshots` must be written to tolerate a repeated or
  out-of-order-by-duplication record. This is why `seq`/timestamp fields exist on
  every schema (§5) rather than being an afterthought.
- Throughput has a ceiling tied to `producer.Flush`'s synchronous wait each batch
  (only after every derived produce in a batch acks does the loop commit and move
  on) - `docs/benchmark.md`'s S1 identifies the real ceiling on this hardware as
  ~50,000 events/s, single-core-bound in the consume loop, not by this
  produce-then-commit synchronization itself, but it is part of the same critical
  path.
- This is scoped to `pm.raw → processor → {pm.ticks,pm.alerts,pm.snapshots}` only.
  The gateway's own Kafka consumption (ADR-002) never commits offsets at all - it
  is a broadcast reader with no notion of "processed", so at-least-once/exactly-once
  simply does not apply there.
