#!/usr/bin/env bash
# Measures database load per event: runs a fixed load through one coordinator and one worker,
# then reports every statement the platform sent to the DSQL stand-in (pg_stat_statements).
#
#   scripts/db-load.sh                      # tracing on (TRACE_SAMPLE=1.0)
#   TRACE_SAMPLE=0 scripts/db-load.sh       # tracing off
#   TRACE_SAMPLE=0.01 RATE=200 DURATION=60 scripts/db-load.sh
set -uo pipefail

TRACE_SAMPLE=${TRACE_SAMPLE:-1.0}
RATE=${RATE:-100}
SECONDS_=${DURATION:-60}
KEYS=${KEYS:-200}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUN_ID=${RUN_ID:-dbload$(date +%H%M%S)}
RUN_DIR="$ROOT/build/dbload/$RUN_ID"
export JAVA_HOME=${JAVA_HOME:-/opt/homebrew/opt/openjdk@25}
export PATH="/opt/homebrew/bin:$PATH"
export KINESIS_ENDPOINT=${KINESIS_ENDPOINT:-http://localhost:4566}
export TRACE_SAMPLE INGEST_POSITION=LATEST INGEST_SHARD_LIST_SEC=5

log() { echo "[db-load $(date +%H:%M:%S)] $*"; }
psql_() { (cd "$ROOT" && docker compose exec -T dsql psql -U concert -d concert -At -F $'\t' -c "$1"); }

mkdir -p "$RUN_DIR"
log "run $RUN_ID: ${SECONDS_}s @ ${RATE}/s, TRACE_SAMPLE=$TRACE_SAMPLE"
(cd "$ROOT" && docker compose up -d >/dev/null 2>&1) || exit 1
until psql_ "SELECT 1" >/dev/null 2>&1; do sleep 1; done
psql_ "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null
(cd "$ROOT" && ./gradlew -q :orchestration:installDist :sample-workers:installDist :tools:installDist) || exit 1

pkill -TERM -f "orchestration/build/install|sample-workers/build/install" 2>/dev/null && sleep 3
(cd "$ROOT" && docker compose exec -T temporal temporal workflow terminate --address temporal:7233 \
  --workflow-id ingest:concert-events --reason "db-load $RUN_ID" >/dev/null 2>&1)

"$ROOT/orchestration/build/install/orchestration/bin/orchestration" > "$RUN_DIR/coordinator.log" 2>&1 & C=$!
SM_TYPE=ledger "$ROOT/sample-workers/build/install/sample-workers/bin/sample-workers" > "$RUN_DIR/worker.log" 2>&1 & W=$!
trap 'kill $C $W 2>/dev/null' EXIT
log "coordinator + worker starting"; sleep 12
for pid in $C $W; do kill -0 "$pid" 2>/dev/null || { log "a process died at startup, see $RUN_DIR/*.log"; exit 1; }; done

psql_ "SELECT pg_stat_statements_reset()" >/dev/null
COMMITS0=$(psql_ "SELECT xact_commit FROM pg_stat_database WHERE datname = 'concert'")

"$ROOT/tools/build/install/tools/bin/tools" loadgen --run-id "$RUN_ID" --rate "$RATE" --seconds "$SECONDS_" \
  --keys "$KEYS" --out "$RUN_DIR/manifest.json" > "$RUN_DIR/loadgen.log" 2>&1
TOTAL=$((RATE * SECONDS_))
log "published $TOTAL events; waiting for all to be applied"
for _ in $(seq 1 120); do
  APPLIED=$(psql_ "SELECT coalesce(sum((data::json->>'count')::bigint), 0) /*db-load-poll*/ FROM sm_state WHERE workflow_id LIKE 'ledger:$RUN_ID-%'")
  [ "$APPLIED" -ge "$TOTAL" ] && break
  sleep 2
done
sleep 2 # let async trace batches flush
COMMITS1=$(psql_ "SELECT xact_commit FROM pg_stat_database WHERE datname = 'concert'")

echo
echo "================ DB load: $RUN_ID  (TRACE_SAMPLE=$TRACE_SAMPLE, $TOTAL events @ ${RATE}/s, applied $APPLIED) ================"
printf "%-13s %-28s %9s %10s %10s %10s\n" "component" "statement" "calls" "per event" "rows" "total ms"
psql_ "
WITH s AS (
  SELECT query, calls, rows, total_exec_time FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = 'concert')
    AND query NOT ILIKE '%pg_stat%' AND query NOT ILIKE '%db-load-poll%' AND query NOT ILIKE '%xact_commit%'
    AND query NOT ILIKE 'SELECT \$1' AND query NOT ILIKE 'SELECT 1'),
c AS (
  SELECT CASE
      WHEN query ILIKE 'INSERT INTO processed_event%'       THEN 'coordinator|dedupe claim'
      WHEN query ILIKE 'SELECT event_id FROM processed%'    THEN 'coordinator|dedupe replay check'
      WHEN query ILIKE 'UPDATE processed_event%'            THEN 'coordinator|mark dispatched'
      WHEN query ILIKE '%shard_checkpoint%'                 THEN 'coordinator|shard checkpoint'
      WHEN query ILIKE 'INSERT INTO sm_state%'              THEN 'worker|state upsert'
      WHEN query ILIKE 'INSERT INTO event_trace%'           THEN 'trace|trace insert'
      WHEN query ~* '^(BEGIN|COMMIT|ROLLBACK)'              THEN 'tx control|BEGIN/COMMIT'
      ELSE 'other|' || left(regexp_replace(query, '\s+', ' ', 'g'), 26) END AS k,
    calls, rows, total_exec_time FROM s)
SELECT split_part(k, '|', 1), split_part(k, '|', 2), sum(calls), round(sum(calls)::numeric / $TOTAL, 3),
       sum(rows), round(sum(total_exec_time)::numeric, 0)
FROM c GROUP BY k ORDER BY 1, 3 DESC" | while IFS=$'\t' read -r comp stmt calls per rows ms; do
  printf "%-13s %-28s %9s %10s %10s %10s\n" "$comp" "$stmt" "$calls" "$per" "$rows" "$ms"
done
psql_ "
SELECT sum(calls), round(sum(calls)::numeric / $TOTAL, 3) FROM pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = 'concert')
  AND query NOT ILIKE '%pg_stat%' AND query NOT ILIKE '%db-load-poll%' AND query NOT ILIKE '%xact_commit%'
  AND query !~* '^(BEGIN|COMMIT|ROLLBACK)'" | { IFS=$'\t' read -r calls per
  printf "%-42s %9s %10s\n" "TOTAL statements (excl. tx control)" "$calls" "$per"; }
printf "%-42s %9s %10s\n" "TOTAL commits (pg_stat_database)" "$((COMMITS1 - COMMITS0))" \
  "$(echo "scale=3; ($COMMITS1 - $COMMITS0) / $TOTAL" | bc)"
