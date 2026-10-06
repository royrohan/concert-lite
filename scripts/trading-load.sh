#!/usr/bin/env bash
# Fire trading order flow into the running stack (trading showcase):
#   scripts/trading-load.sh [orders=100] [rate=50] [extra TradingLoadGen args, e.g. --symbols 10 --concurrency 40]
# Runs the one-shot trading-loadgen container (profile trading-load): events go to Kinesis concert-events,
# the coordinator routes them to the trading worker (scripts/up.sh <store> --analytics --trading). Seed and
# vol multiplier match marketdata-sim (TRADING_SEED / TRADING_VOL_MULT, default 42 / 4), and events are
# stamped on the wall clock, so fills line up with md.ticks.
set -euo pipefail
cd "$(dirname "$0")/.."
orders=${1:-100}
rate=${2:-50}
shift 2 || shift $# || true
docker compose --profile trading-load build -q trading-loadgen
TRADING_ORDERS=$orders TRADING_RATE=$rate docker compose --profile trading-load run --rm trading-loadgen "$@"
echo
echo "Orders close after their allocations. Watch them in Temporal (http://localhost:8080, WorkflowId STARTS_WITH 'trading_order:')"
echo "and in the trace UI Analytics tab (http://localhost:8088/#analytics/clickhouse): trading_fill_slippage, trading_account_positions."
