#!/usr/bin/env bash
# External side effects under chaos: the payments flow (showcases/payments) charges a ledger REST service and
# publishes to Kafka from inside a Temporal worker. Each scenario injects a fault where the outcome of a write is
# ambiguous (the service committed, but the answer never came back), sends payments, and verifies every payment
# against the ledger and the Kafka topic: no double charge, no settlement without a charge, no charge left behind
# by a failed payment, every payment with an outcome.
#
#   scripts/payments-chaos.sh                          # every scenario
#   scripts/payments-chaos.sh commit-then-500 kafka-ack-loss
#   COUNT=60 RATE=5 FAULT_RATE=0.5 scripts/payments-chaos.sh
#   SKIP_DEPLOY=1 SKIP_BUILD=1 scripts/payments-chaos.sh   # the flow is already deployed / built
#
# Scenarios (ledger faults hit FAULT_RATE of the requests, retries included):
#   baseline              no faults
#   commit-then-500       the ledger commits the charge, then answers 500
#   commit-then-timeout   the ledger commits, then answers after 8 s (the worker gives up after 4 s)
#   commit-then-drop      the ledger commits, then drops the connection without an answer
#   fail-before-commit    the ledger answers 503 without committing
#   kafka-ack-loss        Kafka writes the record, the acknowledgement is lost
#   worker-kill           kill -9 a payments worker every KILL_EVERY s, mid-activity
#   kafka-pause           Kafka frozen for 15 s (publishes retry until it is back)
#   kafka-outage          Kafka frozen for 70 s, past the 40 s publish budget: the Saga refunds and reverses
#   storm                 commit-then-drop + kafka-ack-loss + worker-kill together
#   control-no-idempotency  commit-then-500 with the ledger's idempotency keys OFF: double charges are expected
#                         (the run passes when the verifier finds them; it shows what the keys prevent)
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
COUNT=${COUNT:-40}
RATE=${RATE:-4}
FAULT_RATE=${FAULT_RATE:-0.4}
WORKERS=${WORKERS:-2}
KILL_EVERY=${KILL_EVERY:-6}
SKIP_DEPLOY=${SKIP_DEPLOY:-0}
SKIP_BUILD=${SKIP_BUILD:-0}
ALL=(baseline commit-then-500 commit-then-timeout commit-then-drop fail-before-commit kafka-ack-loss worker-kill
  kafka-pause kafka-outage storm control-no-idempotency)
SCENARIOS=("$@")
[ ${#SCENARIOS[@]} -eq 0 ] && SCENARIOS=("${ALL[@]}")

RUN_ID=${RUN_ID:-pchaos$(date +%H%M%S)}
OUT="$ROOT/build/payments-chaos/$RUN_ID"
mkdir -p "$OUT"
LOG="$OUT/report.txt"
LEDGER=http://localhost:8097
export LEDGER_URL=$LEDGER
export KINESIS_ENDPOINT=${KINESIS_ENDPOINT:-http://localhost:4566}
export AWS_REGION=${AWS_REGION:-us-east-1}
# Kafka as the verifier on this host sees it; the containers keep their own (compose must not see this one)
HOST_KAFKA=${KAFKA_HOST_BOOTSTRAP:-localhost:29092}
store=$(grep -E '^STORE_KIND=' .env 2>/dev/null | cut -d= -f2); store=${store:-postgres}
export COMPOSE_PROFILES="$store,apps,payments"
# KAFKA_BOOTSTRAP as deployed with --no-analytics (empty: no analytics snapshots), so scaling does not recreate the workers
dc() { KAFKA_BOOTSTRAP="${DEPLOYED_KAFKA_BOOTSTRAP-}" docker compose -f docker-compose.yml -f showcases/payments/compose.yml \
  -f showcases/payments/compose.extra.yml "$@"; }
CHECK="$ROOT/showcases/payments/build/install/payments/bin/payments-chaos"

say() { echo "$*" | tee -a "$LOG"; }
chaos() { curl -sf -X POST "$LEDGER/chaos" -d "$1" >/dev/null || say "  (could not set chaos $1)"; }
reset() { chaos '{"mode":"none","rate":0,"idempotency":true,"kafkaAckLossRate":0}'; }

if [ "$SKIP_DEPLOY" != 1 ]; then
  say "==> deploying the payments flow"
  ./deploy-concert-ecosystem payments --no-analytics 2>&1 | tail -3
fi
if [ "$SKIP_BUILD" != 1 ]; then
  . scripts/java-env.sh
  ./gradlew -q --console=plain :payments:installDist
fi
say "==> $WORKERS payments workers"
dc up -d --no-deps --scale payments-worker="$WORKERS" payments-worker >/dev/null 2>&1
for _ in $(seq 1 30); do curl -sf "$LEDGER/health" >/dev/null && break; sleep 1; done

workers() { dc ps -q payments-worker 2>/dev/null; }
kafka_id() { dc ps -q kafka 2>/dev/null; }

# kill -9 a random worker every KILL_EVERY s, restart it 3 s later, until $OUT/stop-kills exists
kill_loop() {
  while [ ! -f "$OUT/stop-kills" ]; do
    sleep "$KILL_EVERY"
    [ -f "$OUT/stop-kills" ] && break
    victim=$(workers | shuf -n1)
    [ -n "$victim" ] || continue
    echo "  $(date +%T) kill -9 worker ${victim:0:12}" >> "$LOG"
    docker kill -s KILL "$victim" >/dev/null 2>&1
    sleep 3
    docker start "$victim" >/dev/null 2>&1
  done
}

# freeze Kafka for $1 s after a 5 s head start
pause_kafka() {
  sleep 5
  k=$(kafka_id)
  echo "  $(date +%T) docker pause kafka for $1 s" >> "$LOG"
  docker pause "$k" >/dev/null 2>&1
  sleep "$1"
  docker unpause "$k" >/dev/null 2>&1
  echo "  $(date +%T) kafka back" >> "$LOG"
}

run() {
  local name=$1; shift
  say ""
  KAFKA_BOOTSTRAP=$HOST_KAFKA "$CHECK" --scenario "$name" --count "$COUNT" --rate "$RATE" "$@" 2>&1 | grep -v "JAVA_TOOL_OPTIONS\|SLF4J\|^\[" | tee -a "$LOG"
}

for s in "${SCENARIOS[@]}"; do
  reset
  rm -f "$OUT/stop-kills"
  case $s in
    baseline) run "$s" ;;
    commit-then-500|commit-then-timeout|commit-then-drop|fail-before-commit)
      chaos "{\"mode\":\"$s\",\"rate\":$FAULT_RATE}"; run "$s" ;;
    kafka-ack-loss) chaos "{\"kafkaAckLossRate\":$FAULT_RATE}"; run "$s" ;;
    worker-kill) kill_loop & k=$!; run "$s"; touch "$OUT/stop-kills"; wait $k ;;
    kafka-pause) pause_kafka 15 & p=$!; run "$s"; wait $p ;;
    kafka-outage) pause_kafka 70 & p=$!; run "$s" --timeout 400; wait $p ;;
    storm)
      chaos "{\"mode\":\"commit-then-drop\",\"rate\":0.3,\"kafkaAckLossRate\":0.3}"
      kill_loop & k=$!; run "$s" --timeout 400; touch "$OUT/stop-kills"; wait $k ;;
    control-no-idempotency)
      chaos "{\"mode\":\"commit-then-500\",\"rate\":$FAULT_RATE,\"idempotency\":false}"; run "$s" --expect-violations ;;
    *) say "unknown scenario $s (one of: ${ALL[*]})"; continue ;;
  esac
  # every worker back up before the next scenario
  for w in $(dc ps -aq payments-worker); do docker start "$w" >/dev/null 2>&1; done
done
reset

say ""
say "== summary ($LOG)"
grep '^RESULT' "$LOG" | sed 's/^RESULT //' | python3 -c '
import sys
rows = [dict(kv.split("=", 1) for kv in line.split()) for line in sys.stdin]
if not rows:
    print("no results"); sys.exit()
cols = ["scenario", "payments", "settled", "declined", "failed", "stuck", "dupCommits", "replays", "faults",
        "ledgerRetries", "kafkaRecords", "kafkaDistinct", "kafkaRetries", "violations", "verdict"]
w = {c: max(len(c), *(len(r.get(c, "")) for r in rows)) for c in cols} if rows else {}
print("  ".join(c.ljust(w[c]) for c in cols))
for r in rows:
    print("  ".join(r.get(c, "").ljust(w[c]) for c in cols))
' | tee -a "$LOG"
! grep -q 'verdict=FAIL' "$LOG"
