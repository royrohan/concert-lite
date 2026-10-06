#!/usr/bin/env bash
# Runs a generated ecosystem's event sender (bin/events of showcases/<name>), building it when its sources
# changed. Used by showcases/<name>/send and send-flow:
#   scripts/ecosystem-events.sh <name> send <smType> <eventType> <instanceKey> [payload.json | - | '{json}']
#   scripts/ecosystem-events.sh <name> flow <flow.json> [--delay-ms N]
#   scripts/ecosystem-events.sh <name> list
# Without a payload, send uses samples/<smType>/<eventType>.json. KINESIS_ENDPOINT defaults to LocalStack.
set -euo pipefail
if [ $# -lt 2 ]; then
  sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi
name=$1
shift
root=$(cd "$(dirname "$0")/.." && pwd)
mod="$root/showcases/$name"
bin="$mod/build/install/$name/bin/events"
. "$root/scripts/java-env.sh"
if [ ! -x "$bin" ] || [ -n "$(find "$mod/src/main" "$mod/build.gradle.kts" -newer "$bin" 2>/dev/null | head -1)" ]; then
  echo "building :$name ..." >&2
  (cd "$root" && ./gradlew -q --console=plain ":$name:installDist")
fi
export KINESIS_ENDPOINT=${KINESIS_ENDPOINT:-http://localhost:4566}
export AWS_REGION=${AWS_REGION:-us-east-1}
if [ "$1" = "send" ] && [ $# -eq 4 ]; then
  sample="$mod/samples/$2/$3.json"
  if [ -f "$sample" ]; then
    set -- "$@" "$sample"
  fi
fi
exec "$bin" "$@"
