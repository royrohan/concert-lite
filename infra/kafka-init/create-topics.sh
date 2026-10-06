#!/usr/bin/env bash
# One-shot (compose service kafka-init): creates every topic of the analytics profile, idempotently.
#   entity-snapshots   completed-entity snapshots (concert workers -> sinks); compacted, key = entity id
#   entity-snapshots.dlq  records a sink could not decode / apply (headers error, target, attempts, original.*); 7 days
#   ref.*, md.*        market data from marketdata-sim (bypasses concert); configs as in MdTopics
set -euo pipefail
BS=${KAFKA_BOOTSTRAP:-kafka:9092}
topic() { # name partitions config...
  local name=$1 parts=$2; shift 2
  local args=()
  for c in "$@"; do args+=(--config "$c"); done
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --create --if-not-exists --topic "$name" \
    --partitions "$parts" --replication-factor 1 "${args[@]}"
}
topic entity-snapshots 6 cleanup.policy=compact min.compaction.lag.ms=3600000 max.message.bytes=4194304
topic entity-snapshots.dlq 1 cleanup.policy=delete retention.ms=604800000 max.message.bytes=8388608
topic ref.instruments 1 cleanup.policy=compact
topic ref.accounts    1 cleanup.policy=compact
topic ref.venues      1 cleanup.policy=compact
topic md.ticks        6 cleanup.policy=delete retention.ms=3600000 segment.ms=600000
topic md.bars.1m      3 cleanup.policy=delete retention.ms=86400000
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --list
