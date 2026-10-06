#!/usr/bin/env bash
# Reconcile the analytics sinks with the StateStore now (the Temporal Schedules reconcile-<smType> also run it
# hourly):
#   scripts/reconcile.sh [--smType order] [--dry-run]
# For every typed smType with a running worker (or just --smType), ReconcileWorkflow runs in that worker: it
# scans the StateStore for entities in a terminal state, asks the DuckDB sink for their stored versions
# (read-only POST /versions) and republishes the CURRENT snapshot of missing or stale ones to Kafka
# (entity-snapshots), from where every sink picks it up. --dry-run only counts. Started through the trace UI
# (TRACE_UI_URL, default http://localhost:8088), which waits for the workflows and returns their reports.
set -euo pipefail
url=${TRACE_UI_URL:-http://localhost:8088}
sm=""
dry=false
while [ $# -gt 0 ]; do
  case $1 in
    --smType|--sm-type) [ $# -ge 2 ] || { echo "--smType needs a value" >&2; exit 2; }; sm=$2; shift 2 ;;
    --smType=*|--sm-type=*) sm=${1#*=}; shift ;;
    --dry-run) dry=true; shift ;;
    -h|--help) sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument $1 (usage: scripts/reconcile.sh [--smType X] [--dry-run])" >&2; exit 2 ;;
  esac
done
resp=$(curl -sf -X POST "$url/api/analytics/reconcile?dryRun=$dry${sm:+&smType=$sm}") || {
  echo "trace UI not reachable at $url (scripts/up.sh <store> --analytics starts it)" >&2; exit 1; }
python3 - "$resp" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
t = d.get("totals", {})
print(f"reconcile{' (dry run)' if d.get('dryRun') else ''} at {d.get('at')} in {d.get('elapsedMs')} ms")
print(f"{'smType':<22}{'scanned':>9}{'terminal':>10}{'missing':>9}{'stale':>7}{'ahead':>7}{'republished':>13}  note")
for r in d.get("reports", []):
    if "error" in r:
        print(f"{r['smType']:<22}  ERROR {r['error']}")
        continue
    note = r.get("skipped") or f"{r['elapsedMs']} ms"
    print(f"{r['smType']:<22}{r['scanned']:>9}{r['terminal']:>10}{r['missing']:>9}{r['stale']:>7}{r['ahead']:>7}{r['republished']:>13}  {note}")
print(f"{'TOTAL':<22}{t.get('scanned',0):>9}{t.get('terminal',0):>10}{t.get('missing',0):>9}{t.get('stale',0):>7}{t.get('ahead',0):>7}{t.get('republished',0):>13}")
for s in d.get("skipped", []):
    if not any(r.get("smType") == s["smType"] for r in d.get("reports", [])):
        print(f"skipped {s['smType']}: {s['reason']}")
sys.exit(1 if any("error" in r for r in d.get("reports", [])) else 0)
PY
