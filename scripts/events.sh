#!/usr/bin/env bash
# Operator commands for event-style events (lifecycle rows event:<id> in the StateStore, processors evproc:*):
#   scripts/events.sh list [STATUS|errors|all] [idPrefix]   STATUS: DONE ERROR_BLOCKING ERROR_NON_BLOCKING SCHEDULED NEW
#   scripts/events.sh show <eventId>                        the lifecycle row as JSON
#   scripts/events.sh retry <eventId>                       blocked: runs again now; parked: re-enters its lock chain
#   scripts/events.sh skip <eventId> [reason...]            goes DONE without running (a blocked one frees its keys)
#   scripts/events.sh status <eventId>                      live status of the event's processor
# Runs bin/events of :orchestration (built when its sources changed) against the local stack by default
# (STORE_KIND / STORE_JDBC_URL, TEMPORAL_ADDRESS as for the apps).
set -euo pipefail
if [ $# -lt 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi
root=$(cd "$(dirname "$0")/.." && pwd)
mod="$root/platform/orchestration"
bin="$mod/build/install/orchestration/bin/events"
. "$root/scripts/java-env.sh"
if [ ! -x "$bin" ] || [ -n "$(find "$mod/src/main" "$root/core/common/src/main" "$root/platform/store/src/main" \
    "$mod/build.gradle.kts" -newer "$bin" 2>/dev/null | head -1)" ]; then
  echo "building :orchestration ..." >&2
  (cd "$root" && ./gradlew -q --console=plain :orchestration:installDist)
fi
# Default to the store the running compose stack uses, so `events.sh list` just works after up/deploy.
if [ -z "${STORE_KIND:-}" ]; then
  running=$(docker ps --format '{{.Names}}' 2>/dev/null || true)
  case "$running" in
    *concert-temporal-spanner-*) STORE_KIND=spanner ;;
    *concert-temporal-dsql-*) STORE_KIND=postgres ;;
    *) STORE_KIND=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' concert-temporal-coordinator-1 2>/dev/null \
         | sed -n 's/^STORE_KIND=//p'); STORE_KIND=${STORE_KIND:-postgres} ;;
  esac
fi
export STORE_KIND
export SPANNER_EMULATOR_HOST=${SPANNER_EMULATOR_HOST:-localhost:9010}
export DYNAMO_ENDPOINT=${DYNAMO_ENDPOINT:-http://localhost:4566}
export STORE_JDBC_URL=${STORE_JDBC_URL:-jdbc:postgresql://localhost:5433/concert}
export STORE_INIT_SCHEMA=${STORE_INIT_SCHEMA:-false}
export TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-localhost:7233}
export JAVA_OPTS="${JAVA_OPTS:-} -Dorg.slf4j.simpleLogger.defaultLogLevel=warn"
exec "$bin" "$@"
