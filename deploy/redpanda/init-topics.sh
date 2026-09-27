#!/bin/bash
# Creates the OddsPulse topics (design plan section 5). Safe to re-run.
set -euo pipefail

BROKERS="${KAFKA_BROKERS:-redpanda:9092}"

H6=21600000      # 6 hours in ms
H24=86400000     # 24 hours
D7=604800000     # 7 days
H1=3600000       # 1 hour

# create <topic> <partitions> <config>...
create() {
  local topic="$1" partitions="$2"; shift 2
  local args=()
  for c in "$@"; do args+=(-c "$c"); done
  if out=$(rpk -X brokers="$BROKERS" topic create "$topic" -p "$partitions" -r 1 "${args[@]}" 2>&1); then
    echo "created $topic"
  elif grep -q TOPIC_ALREADY_EXISTS <<<"$out"; then
    echo "exists  $topic"
  else
    echo "$out" >&2
    return 1
  fi
}

create pm.markets   3  cleanup.policy=compact
create pm.raw       12 cleanup.policy=delete retention.ms=$H6
create pm.ticks     12 cleanup.policy=delete retention.ms=$H24
create pm.snapshots 12 cleanup.policy=compact
create pm.alerts    3  cleanup.policy=delete retention.ms=$D7
create pm.top       1  cleanup.policy=delete retention.ms=$H1

rpk -X brokers="$BROKERS" topic list
