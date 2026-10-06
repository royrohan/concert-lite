#!/usr/bin/env bash
# Chaos test: run coordinators and workers as real processes, randomly kill / restart them while a
# load generator publishes events, then verify nothing was lost, double-applied or reordered.
#
#   scripts/chaos.sh                         # defaults below
#   DURATION=300 RATE=200 CHAOS_TEMPORAL=1 scripts/chaos.sh
#   STORE_KIND=spanner scripts/chaos.sh      # same experiment on another backend
#   EVENT_STYLE=1 scripts/chaos.sh           # event-style: DemoEvents Ticks (handlers, keyed state, chained
#                                            # child events) instead of ledger state machines
#
# What it exercises (all Temporal out of the box, no custom failover code):
#   kill -9 coordinator  -> shard-consumer activity heartbeat times out, retried on the other coordinator
#                           from its last heartbeat checkpoint; lock workflow tasks move off the dead
#                           worker's sticky queue and replay elsewhere
#   SIGTERM coordinator  -> graceful: consumers are interrupted and retried immediately
#   kill -9 worker       -> entity workflow tasks / local activities time out and are retried on the
#                           surviving worker; update ids make the retried transition idempotent
#   whole tier down      -> work queues durably in Temporal (and Kinesis) until a process comes back
#   restart Temporal     -> (CHAOS_TEMPORAL=1) clients and pollers reconnect; state is in Postgres
set -uo pipefail

DURATION=${DURATION:-180}          # seconds of load
RATE=${RATE:-100}                  # events/s
KEYS=${KEYS:-200}                  # ledgers (even: single-key, odd: multi-key)
KILL_EVERY=${KILL_EVERY:-15}       # seconds between chaos actions
DOWN_MIN=${DOWN_MIN:-3}            # victim stays down this long (random in range)
DOWN_MAX=${DOWN_MAX:-12}
COORDINATORS=${COORDINATORS:-2}
WORKERS=${WORKERS:-2}
CHAOS_TEMPORAL=${CHAOS_TEMPORAL:-0}
KEEP=${KEEP:-0}                    # 1 = leave processes running afterwards
EVENT_STYLE=${EVENT_STYLE:-0}      # 1 = event-style load (workers run the demo event domain, ev-demo)
CHAIN_EVERY=${CHAIN_EVERY:-5}      # event style: every n-th Tick of a key emits a ChainTick

ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUN_ID=${RUN_ID:-chaos$(date +%H%M%S)}
RUN_DIR="$ROOT/build/chaos/$RUN_ID"
CHAOS_LOG="$RUN_DIR/chaos.log"
export JAVA_HOME=${JAVA_HOME:-/opt/homebrew/opt/openjdk@25}
export PATH="/opt/homebrew/bin:$PATH"
export KINESIS_ENDPOINT=${KINESIS_ENDPOINT:-http://localhost:4566}
export TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-localhost:7233}
export STORE_KIND=${STORE_KIND:-postgres}            # postgres | dynamo | spanner
export STORE_JDBC_URL=${STORE_JDBC_URL:-jdbc:postgresql://localhost:5433/concert}
export DYNAMO_ENDPOINT=${DYNAMO_ENDPOINT:-http://localhost:4566}
export SPANNER_EMULATOR_HOST=${SPANNER_EMULATOR_HOST:-localhost:9010}
case $STORE_KIND in                                 # infra only: the apps run as local processes here
  postgres) export COMPOSE_PROFILES=postgres ;;
  spanner)  export COMPOSE_PROFILES=spanner ;;
  *)        export COMPOSE_PROFILES= ;;
esac
export INGEST_SHARD_LIST_SEC=${INGEST_SHARD_LIST_SEC:-5}

ORCH="$ROOT/platform/orchestration/build/install/orchestration/bin/orchestration"
WORKER="$ROOT/showcases/sample-workers/build/install/sample-workers/bin/sample-workers"
TOOLS="$ROOT/showcases/tools/build/install/tools/bin/tools"

log() { echo "[chaos $(date +%H:%M:%S)] $*"; }
mark() { echo "$(($(date +%s) * 1000)) $1 $2" >> "$CHAOS_LOG"; log "$1 $2"; }

procs() {
  local i
  for i in $(seq 1 "$COORDINATORS"); do echo "coord-$i"; done
  for i in $(seq 1 "$WORKERS"); do echo "worker-$i"; done
}

start_proc() {
  local name=$1
  case $name in
    coord-*)  nohup "$ORCH" >> "$RUN_DIR/$name.log" 2>&1 & ;;
    worker-*)
      if [ "$EVENT_STYLE" = "1" ]; then
        SM_TYPE=none EVENT_DOMAINS=demo nohup "$WORKER" >> "$RUN_DIR/$name.log" 2>&1 &
      else
        SM_TYPE=ledger EVENT_DOMAINS= nohup "$WORKER" >> "$RUN_DIR/$name.log" 2>&1 &
      fi ;;
  esac
  echo $! > "$RUN_DIR/$name.pid"
  mark START "$name"
}

stop_proc() {  # stop_proc <name> <signal>
  local name=$1 sig=$2 pid
  pid=$(cat "$RUN_DIR/$name.pid" 2>/dev/null) || return 0
  # The start script execs java under the same pid on macOS; also catch any child JVM.
  pkill -"$sig" -P "$pid" 2>/dev/null
  kill -"$sig" "$pid" 2>/dev/null
  rm -f "$RUN_DIR/$name.pid"
}

is_up() { [ -f "$RUN_DIR/$1.pid" ]; }

cleanup() {
  if [ "$KEEP" = "1" ]; then log "KEEP=1: leaving processes running (pids in $RUN_DIR)"; return; fi
  log "stopping all processes"
  for p in $(procs); do stop_proc "$p" TERM; done
  [ -n "${LOADGEN_PID:-}" ] && kill "$LOADGEN_PID" 2>/dev/null
}
trap cleanup EXIT

# ---------------------------------------------------------------- setup
mkdir -p "$RUN_DIR"; : > "$CHAOS_LOG"
STYLE=entity; [ "$EVENT_STYLE" = "1" ] && STYLE=event
log "run $RUN_ID (store: $STORE_KIND, style: $STYLE): ${DURATION}s @ ${RATE}/s over $KEYS keys, chaos every ${KILL_EVERY}s, $COORDINATORS coordinators, $WORKERS workers"
log "logs: $RUN_DIR"

(cd "$ROOT" && docker compose up -d >/dev/null 2>&1) || { log "docker compose up failed"; exit 1; }
for _ in $(seq 1 60); do
  (cd "$ROOT" && docker compose exec -T temporal temporal operator cluster health --address temporal:7233 >/dev/null 2>&1) && break
  sleep 2
done
log "building distributions"
(cd "$ROOT" && ./gradlew -q :orchestration:installDist :sample-workers:installDist :tools:installDist) || exit 1

if pgrep -f "orchestration/build/install|sample-workers/build/install" >/dev/null; then
  log "stopping previously started coordinators/workers so they don't join the experiment"
  pkill -TERM -f "orchestration/build/install|sample-workers/build/install"; sleep 3
fi

# Start from a fresh ingest supervisor so it picks up the current failover settings; consumers
# resume from the DSQL checkpoints.
(cd "$ROOT" && docker compose exec -T temporal temporal workflow terminate --address temporal:7233 \
  --workflow-id ingest:concert-events --reason "chaos run $RUN_ID" >/dev/null 2>&1) && log "reset ingest supervisor"

for p in $(procs); do start_proc "$p"; done
log "waiting for processes to come up"; sleep 8

# ---------------------------------------------------------------- load + chaos
"$TOOLS" loadgen --run-id "$RUN_ID" --rate "$RATE" --seconds "$DURATION" --keys "$KEYS" \
  --style "$STYLE" --chain-every "$CHAIN_EVERY" --out "$RUN_DIR/manifest.json" > "$RUN_DIR/loadgen.log" 2>&1 &
LOADGEN_PID=$!

random_victim() {  # random_victim <prefix or empty>
  local list n
  list=$(procs | grep "^${1:-}" | while read -r p; do is_up "$p" && echo "$p"; done)
  n=$(echo "$list" | grep -c .)
  [ "$n" -eq 0 ] && return 1
  echo "$list" | sed -n "$((RANDOM % n + 1))p"
}

while kill -0 "$LOADGEN_PID" 2>/dev/null; do
  sleep "$KILL_EVERY"
  kill -0 "$LOADGEN_PID" 2>/dev/null || break
  down=$((DOWN_MIN + RANDOM % (DOWN_MAX - DOWN_MIN + 1)))
  roll=$((RANDOM % 100))
  victims=""
  if   [ $roll -lt 40 ]; then v=$(random_victim) && { stop_proc "$v" KILL; mark KILL9 "$v"; victims=$v; }
  elif [ $roll -lt 60 ]; then v=$(random_victim) && { stop_proc "$v" TERM; mark SIGTERM "$v"; victims=$v; }
  elif [ $roll -lt 75 ]; then
    for v in $(procs | grep coord-); do stop_proc "$v" KILL; victims="$victims $v"; done; mark KILL9 "all-coordinators"
  elif [ $roll -lt 90 ]; then
    for v in $(procs | grep worker-); do stop_proc "$v" KILL; victims="$victims $v"; done; mark KILL9 "all-workers"
  elif [ "$CHAOS_TEMPORAL" = "1" ]; then
    mark RESTART_TEMPORAL "temporal-server"
    (cd "$ROOT" && docker compose restart temporal >/dev/null 2>&1)
  else
    v=$(random_victim) && { stop_proc "$v" KILL; mark KILL9 "$v"; victims=$v; }
  fi
  [ -n "$victims" ] && { sleep "$down"; for v in $victims; do start_proc "$v"; done; }
done
wait "$LOADGEN_PID"; log "load finished: $(tail -1 "$RUN_DIR/loadgen.log")"

# make sure everything is up for the drain
for p in $(procs); do is_up "$p" || start_proc "$p"; done

# ---------------------------------------------------------------- verify
log "verifying (waits for the backlog to drain)"
"$TOOLS" verify --manifest "$RUN_DIR/manifest.json" --chaos-log "$CHAOS_LOG" --wait-seconds 300 | tee "$RUN_DIR/report.txt"
exit "${PIPESTATUS[0]}"
