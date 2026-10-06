#!/usr/bin/env bash
# Bring up the whole platform on one backend, apps included:
#   scripts/up.sh postgres | dynamo | spanner
#   scripts/up.sh spanner --scale coordinator=2      (extra args go to docker compose up)
#   scripts/up.sh spanner --analytics                (+ Kafka, DuckDB sink, ClickHouse, Deephaven, market data)
#   scripts/up.sh spanner --analytics --trading      (+ trading worker; then scripts/trading-load.sh [orders] [rate])
#   scripts/up.sh down                               (stop everything, keep volumes)
set -euo pipefail
cd "$(dirname "$0")/.."

kind=${1:-$(grep -E '^STORE_KIND=' .env | cut -d= -f2)}
shift || true
if [ "$kind" = "down" ]; then
  COMPOSE_PROFILES=postgres,spanner,apps,analytics,kafka-ui,trading,trading-load docker compose down
  exit 0
fi
case $kind in
  postgres) db_profile=postgres ;;
  spanner)  db_profile=spanner ;;
  dynamo)   db_profile="" ;;          # DynamoDB runs inside localstack
  *) echo "usage: scripts/up.sh postgres|dynamo|spanner|down" >&2; exit 2 ;;
esac

# --analytics: run Kafka + sinks and let workers publish snapshots; otherwise workers get an empty
# KAFKA_BOOTSTRAP and use the no-op publisher (as before).
# --trading: the trading worker (profile "trading"); order flow comes from scripts/trading-load.sh.
analytics=""
trading=""
args=()
for a in "$@"; do
  case $a in
    --analytics) analytics=",analytics" ;;
    --trading) trading=",trading" ;;
    *) args+=("$a") ;;
  esac
done
if [ -n "$analytics" ]; then unset KAFKA_BOOTSTRAP; else export KAFKA_BOOTSTRAP=""; fi

export STORE_KIND=$kind
export COMPOSE_PROFILES=${db_profile:+$db_profile,}apps$analytics$trading
echo "STORE_KIND=$STORE_KIND COMPOSE_PROFILES=$COMPOSE_PROFILES"
docker compose up -d --build ${args[@]+"${args[@]}"}
echo
echo "Temporal UI http://localhost:8080   Trace UI http://localhost:8088   (store: $STORE_KIND)"
if [ -n "$analytics" ]; then
  echo "Analytics (trace UI -> Analytics): DuckDB sink http://localhost:8090   ClickHouse http://localhost:8123/play"
  echo "  Deephaven http://localhost:10000/ide/?psk=${DEEPHAVEN_PSK:-concert}   Kafka localhost:29092"
  echo "  Redpanda Console (optional): COMPOSE_PROFILES=kafka-ui docker compose up -d redpanda-console -> http://localhost:8081"
fi
if [ -n "$trading" ]; then
  echo "Trading: worker for trading_order/_execution/_fill/_allocation is up. Fire order flow with"
  echo "  scripts/trading-load.sh [orders=100] [rate=50]   (events/s; fills priced on the marketdata-sim quotes)"
  if [ -z "$analytics" ]; then
    echo "  (without --analytics there are no ticks to join and no snapshots in the sinks)"
  fi
fi
