#!/usr/bin/env bash
# concert's Pure against legend-lite (pinned and built from source: infra/legend-lite/Dockerfile, compose service
# legend-lite, profile analytics):
#   scripts/legend-lite.sh check    every production .pure model set compiles under real Pure semantics, and the Legend
#                                   store layer generated for each deployable set of sink roots compiles and plans a
#                                   query on DuckDB and ClickHouse (analytics/legend-model LegendLiteCompileTest)
#   scripts/legend-lite.sh store [--models dir,dir] [--roots smType:pkg::Class,...] [--targets duckdb,clickhouse]
#                                   print the generated store layer (Database / Mapping / Connection / Runtime)
# check talks to LEGEND_LITE_URL, default http://localhost:8095; if nothing answers there it starts the compose
# service (the first build of the image takes a while).
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
. scripts/java-env.sh
cmd=${1:-check}
shift || true
url=${LEGEND_LITE_URL:-http://localhost:8095}

healthy() { curl -fsS --max-time 2 "$url/health" 2>/dev/null | grep -q ok; }

case $cmd in
  check)
    if ! healthy; then
      if [ -n "${LEGEND_LITE_URL:-}" ]; then
        echo "legend-lite not reachable at $url" >&2
        exit 1
      fi
      echo "==> starting legend-lite (docker compose service; the first build compiles it with Bazel)" >&2
      COMPOSE_PROFILES=analytics docker compose up -d --build legend-lite >&2
      for _ in $(seq 1 120); do healthy && break; sleep 2; done
      healthy || { echo "legend-lite did not become healthy at $url (docker compose logs legend-lite)" >&2; exit 1; }
    fi
    echo "==> legend-lite at $url"
    LEGEND_LITE_URL=$url ./gradlew -q --console=plain :legend-model:test --tests '*LegendLiteCompileTest*' --rerun -i \
      | grep -E "legend-lite: deployments skipped" || true
    report="analytics/legend-model/build/test-results/test/TEST-io.concert.legend.LegendLiteCompileTest.xml"
    if grep -q '<failure' "$report"; then
      echo "FAILED: see $report" >&2
      exit 1
    fi
    if grep -q 'skipped="[1-9]' "$report"; then
      echo "checks were skipped (no server?): $report" >&2
      exit 1
    fi
    echo "OK: every model set and every deployable store layer compiles; plans on DuckDB and ClickHouse"
    ;;
  store)
    ./gradlew -q --console=plain :legend-model:installDist
    exec analytics/legend-model/build/install/legend-model/bin/legend-model "$@"
    ;;
  *)
    sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
