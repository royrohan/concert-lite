#!/bin/bash
# Registers the custom search attributes used by concert-temporal (idempotent).
set -uo pipefail
for attr in EventId:Keyword SmType:Keyword InstanceKey:Keyword LockKeys:KeywordList; do
  name=${attr%%:*}; type=${attr##*:}
  temporal operator search-attribute create --namespace default --name "$name" --type "$type" 2>&1 \
    | grep -v "already exists" || true
done
temporal operator search-attribute list --namespace default
