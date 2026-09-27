# ADR-001: Use Redpanda instead of Apache Kafka

## Context

OddsPulse needs a Kafka-API-compatible log for the `pm.*` topics (§5 of the design
plan): partitioned, ordered per key, with both `delete` and `compact` retention
policies. The whole backend is meant to run as a single `docker-compose` stack on one
small VPS (2 vCPU / 4 GB target, §13), and the project is a solo, part-time,
portfolio-scale effort — operational simplicity matters more than multi-node
durability guarantees.

Apache Kafka needs either a separate ZooKeeper ensemble or its own KRaft controller
process, a JVM heap sized apart from the page cache, and generally more memory just to
sit idle. Redpanda implements the Kafka wire protocol in a single self-contained C++
binary with no JVM and no separate coordination service, and it exposes `--smp` /
`--memory` flags to cap CPU shards and RAM directly, which matches the "one VPS,
capped resources" constraint in the design plan's risk table (§16: "Redpanda uses too
much memory → OOM").

## Decision

Run a single-node Redpanda broker in `--mode=dev-container` for both local dev and the
VPS deployment, reached over the Kafka API by every Go service via `franz-go`. No
Apache Kafka, no ZooKeeper, no separate KRaft controller.

## Consequences

- One process to run, monitor and restart instead of a JVM broker plus a coordination
  service; `docker compose up -d` brings up the whole log in one container.
- `--smp=1 --memory=1G` bounds Redpanda's footprint predictably on a small VPS.
- Being Kafka-API-compatible, any `franz-go` code written against it also runs
  against real Kafka unchanged — swapping the broker later, if ever needed, is a
  deployment change, not a code change.
- Single node means no replication and no broker-failure tolerance; `pm.*` topics are
  created with `replicas=1` on purpose (see `deploy/redpanda/init-topics.sh`). This is
  an accepted, explicit trade-off for a portfolio project, not something to fix later.
- Redpanda's own admin/schema-registry/HTTP-proxy features and any Kafka behavior
  Redpanda hasn't reimplemented (e.g. certain broker-side quirks) are out of scope;
  only the Kafka produce/consume/admin API surface that `franz-go` and `rpk` use is
  exercised.
