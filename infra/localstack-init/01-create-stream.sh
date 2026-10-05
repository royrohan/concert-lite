#!/bin/bash
# Runs when LocalStack is ready. Shard count: KINESIS_SHARDS (default 4).
set -euo pipefail
awslocal kinesis create-stream --stream-name concert-events --shard-count "${KINESIS_SHARDS:-4}" || true
awslocal kinesis wait stream-exists --stream-name concert-events
echo "concert-events ready"
