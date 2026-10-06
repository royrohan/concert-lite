"""Concert live analytics (Deephaven app mode).

Two Kafka sources, all tables in memory and rebuilt from offset 0 on every start (nothing is committed).
App-mode scripts run in the server's Python console session, so every table below is also a global in the
IDE console (http://localhost:10000/ide/): use them directly in your own code or the starter notebooks.

* entity-snapshots -- completed concert entities {entityId, smType, state, version, createdAt,
  completedAt, model}: latest version per entity, per-type tables, completion analytics.
* ref.* / md.* -- marketdata-sim (bypasses concert): instruments, ticks, quotes, 1-minute bars, VWAP.
  Column specs come from marketdata_columns.json, generated from the trading Pure models
  (sink-clickhouse DeephavenColumns; a test keeps the file in step with the models).
* trading -- completed trading_order / trading_execution / trading_fill / trading_allocation entities
  (the concert path, also from entity-snapshots; columns from the same JSON file) joined with the market
  data: live slippage (aj against ticks), VWAP vs arrival, positions and live P&L per account
  (allocations x latest mid), exposure by sector.
"""
import functools
import json
import os
from typing import Optional

from deephaven import agg
from deephaven import dtypes as dht
from deephaven.appmode import ApplicationState, get_app_state
from deephaven.stream.kafka import consumer as kc

KAFKA = {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")}
HERE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else "/app.d"
TYPES = {"string": dht.string, "long": dht.int64, "double": dht.double, "bool": dht.bool_}


SNAPSHOT_TOPIC = "entity-snapshots"


@functools.lru_cache(maxsize=16384)
def _snapshot_doc(text):
    try:
        doc = json.loads(text)
    except (TypeError, ValueError):
        return None
    return doc if isinstance(doc, dict) else None


def _snapshot_at(text, path):
    node = _snapshot_doc(text)
    for part in path.strip("/").split("/"):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node


def snap_ok(text: str) -> bool:
    doc = _snapshot_doc(text)
    return doc is not None and isinstance(doc.get("entityId"), str) and isinstance(doc.get("smType"), str)


def snap_str(text: str, path: str) -> Optional[str]:
    value = _snapshot_at(text, path)
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"), sort_keys=True)  # objects (the model), numbers, booleans


def snap_long(text: str, path: str) -> Optional[int]:
    value = _snapshot_at(text, path)
    try:
        return None if value is None or isinstance(value, bool) else int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def snap_double(text: str, path: str) -> Optional[float]:
    value = _snapshot_at(text, path)
    try:
        return None if value is None or isinstance(value, bool) else float(value)
    except (TypeError, ValueError, OverflowError):
        return None


def snap_bool(text: str, path: str) -> Optional[bool]:
    value = _snapshot_at(text, path)
    return value if isinstance(value, bool) else None


_SNAPSHOT_READERS = {dht.string: "snap_str", dht.int64: "snap_long", dht.double: "snap_double", dht.bool_: "snap_bool"}


def consume(topic, columns, mapping, table_type):
    """Kafka topic -> table. Market data uses Deephaven's JSON spec directly; entity-snapshots is read as raw
    text and parsed per row, because one malformed record on that compacted topic (the DuckDB sink sends such
    records to the DLQ) would otherwise fail every table built on it. Malformed records are skipped."""
    if topic != SNAPSHOT_TOPIC:
        return kc.consume(KAFKA, topic, offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING, key_spec=kc.KeyValueSpec.IGNORE,
                          value_spec=kc.json_spec(columns, mapping=mapping), table_type=table_type)
    fields = {column: field for field, column in mapping.items()}
    formulas = [f"{column} = {_SNAPSHOT_READERS[kind]}(SnapshotJson, `{fields.get(column, column)}`)"
                for column, kind in columns.items()]
    raw = kc.consume(KAFKA, topic, offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING, key_spec=kc.KeyValueSpec.IGNORE,
                     value_spec=kc.simple_spec("SnapshotJson", dht.string), table_type=table_type)
    return raw.where("snap_ok(SnapshotJson)").update(formulas).drop_columns(["SnapshotJson"])


def parse_instants(table, names):
    """ISO-8601 strings -> Instant (null stays null)."""
    return table.update([f"{n} = parseInstantQuiet({n}Text)" for n in names]).drop_columns([f"{n}Text" for n in names])


# ------------------------------------------------------------------ completed entities
# A few model fields per type are read with JSON pointers; ModelJson keeps the whole model as text.
SNAPSHOT_COLUMNS = {
    "EntityId": dht.string, "SmType": dht.string, "State": dht.string, "Version": dht.int64,
    "CreatedAtText": dht.string, "CompletedAtText": dht.string, "ModelJson": dht.string,
    "TotalAmount": dht.double, "TotalCurrency": dht.string, "CustomerId": dht.string, "CustomerName": dht.string,
    "PaymentMethod": dht.string, "Method": dht.string, "CapturedAmount": dht.double, "CapturedCurrency": dht.string,
    "Carrier": dht.string, "DestinationCountry": dht.string, "InsuredAmount": dht.double,
}
SNAPSHOT_MAPPING = {
    "entityId": "EntityId", "smType": "SmType", "state": "State", "version": "Version",
    "createdAt": "CreatedAtText", "completedAt": "CompletedAtText", "model": "ModelJson",  # object -> raw JSON text
    "/model/total/amount": "TotalAmount", "/model/total/currency": "TotalCurrency",
    "/model/customer/customerId": "CustomerId", "/model/customer/name": "CustomerName",
    "/model/paymentMethod": "PaymentMethod", "/model/method": "Method",
    "/model/captured/amount": "CapturedAmount", "/model/captured/currency": "CapturedCurrency",
    "/model/carrier": "Carrier", "/model/destination/country": "DestinationCountry",
    "/model/insuredValue/amount": "InsuredAmount",
}

snapshots_raw = parse_instants(
    consume("entity-snapshots", SNAPSHOT_COLUMNS, SNAPSHOT_MAPPING, kc.TableType.append()),
    ["CreatedAt", "CompletedAt"])

# Latest version per entity (the topic is compacted and only current versions are published, but a
# replay can still see an older record before a newer one).
snapshots = snapshots_raw.agg_by(
    agg.sorted_last("Version", [c for c in snapshots_raw.column_names if c != "EntityId"]), by=["EntityId"]) \
    .update("LatencyMs = (isNull(CreatedAt) || isNull(CompletedAt)) ? NULL_LONG : diffMillis(CreatedAt, CompletedAt)")

orders_latest = snapshots.where("SmType == `order`").view([
    "EntityId", "State", "Version", "CreatedAt", "CompletedAt", "LatencyMs",
    "CustomerId", "CustomerName", "TotalAmount", "Currency = TotalCurrency", "PaymentMethod"])
payments_latest = snapshots.where("SmType == `payment`").view([
    "EntityId", "State", "Version", "CreatedAt", "CompletedAt", "LatencyMs",
    "Method", "CapturedAmount", "Currency = CapturedCurrency"])
shipments_latest = snapshots.where("SmType == `shipment`").view([
    "EntityId", "State", "Version", "CreatedAt", "CompletedAt", "LatencyMs",
    "Carrier", "DestinationCountry", "InsuredAmount"])

completions_per_minute = snapshots.where("!isNull(CompletedAt)") \
    .update("Minute = lowerBin(CompletedAt, MINUTE)") \
    .count_by("Completions", by=["Minute", "SmType"]).sort_descending(["Minute"])

state_distribution = snapshots.count_by("Entities", by=["SmType", "State"]).sort(["SmType", "State"])

completion_latency = snapshots.where("!isNull(LatencyMs)").agg_by([
    agg.count_("Entities"),
    agg.pct(0.50, ["P50Ms = LatencyMs"]),
    agg.pct(0.95, ["P95Ms = LatencyMs"]),
    agg.max_(["MaxMs = LatencyMs"]),
    agg.avg(["AvgMs = LatencyMs"]),
], by=["SmType"])

delivered_orders = orders_latest.where(["State == `DELIVERED`", "!isNull(TotalAmount)"])
revenue_by_currency = delivered_orders.agg_by([
    agg.count_("Orders"), agg.sum_(["Revenue = TotalAmount"]), agg.avg(["AvgOrder = TotalAmount"])], by=["Currency"])
revenue_by_customer = delivered_orders.agg_by([
    agg.count_("Orders"), agg.sum_(["Revenue = TotalAmount"])], by=["CustomerId", "CustomerName", "Currency"]) \
    .sort_descending(["Revenue"])

# ------------------------------------------------------------------ market data (bypasses concert)
with open(os.path.join(HERE, "marketdata_columns.json")) as f:
    MD = json.load(f)


def md_topic(topic, table_type):
    cols, mapping, instants = {}, {}, []
    for column, field, kind in MD[topic]["columns"]:
        if kind == "instant":
            instants.append(column)
            column = column + "Text"
            kind = "string"
        cols[column] = TYPES[kind]
        mapping[field] = column
    return parse_instants(consume(topic, cols, mapping, table_type), instants)


instruments = md_topic("ref.instruments", kc.TableType.append()).last_by("Symbol")
accounts = md_topic("ref.accounts", kc.TableType.append()).last_by("AccountId")
venues = md_topic("ref.venues", kc.TableType.append()).last_by("Mic")

# Ticks: a ring keeps the newest 300k rows (~25 min at 200 ticks/s) so memory stays bounded.
ticks = md_topic("md.ticks", kc.TableType.ring(300_000))
quotes_latest = ticks.last_by("Symbol").view(["Symbol", "Ts", "Bid", "Ask", "Mid", "Last", "Spread = Ask - Bid", "Volume"])
bars_1m = md_topic("md.bars.1m", kc.TableType.ring(100_000))

vwap_by_symbol = ticks.where("!isNull(LastSize) && LastSize > 0") \
    .update("Notional = Last * LastSize") \
    .agg_by([agg.sum_(["Notional", "TradedQty = LastSize"]), agg.count_("Trades"), agg.last(["Last", "LastTs = Ts"])],
            by=["Symbol"]) \
    .update("Vwap = Notional / TradedQty") \
    .natural_join(quotes_latest, on=["Symbol"], joins=["Mid"]) \
    .update("MidVsVwapBps = (Mid - Vwap) / Vwap * 10000") \
    .sort(["Symbol"])

quotes_with_sector = quotes_latest.natural_join(instruments, on=["Symbol"], joins=["Name", "Sector", "Currency", "RefPrice"]) \
    .update("MoveVsRefPct = (Mid - RefPrice) / RefPrice * 100")
sector_moves = quotes_with_sector.agg_by([agg.count_("Symbols"), agg.avg(["AvgMoveVsRefPct = MoveVsRefPct"])], by=["Sector"]) \
    .sort_descending(["AvgMoveVsRefPct"])

# ------------------------------------------------------------------ trading (concert path) x market data
# The trading aggregates as completed snapshots: one consume of entity-snapshots reading each root's
# fields out of the model with JSON pointers (/model/<field>), latest version per entity, one table per
# smType. Orders carry aggregates only; fills, executions and allocations join on OrderId / ExecId.
TRADING = MD["entity-snapshots"]["roots"]


def trading_snapshots():
    cols = {"EntityId": dht.string, "SmType": dht.string, "State": dht.string, "Version": dht.int64}
    mapping = {"entityId": "EntityId", "smType": "SmType", "state": "State", "version": "Version"}
    instants = set()
    for spec in TRADING.values():
        for column, pointer, kind in spec["columns"]:
            if kind == "instant":
                instants.add(column)
                column, kind = column + "Text", "string"
            cols[column] = TYPES[kind]
            mapping[pointer] = column
    types = ", ".join(f"`{t}`" for t in TRADING)
    raw = consume("entity-snapshots", cols, mapping, kc.TableType.append()).where(f"SmType in {types}")
    latest = raw.agg_by(agg.sorted_last("Version", [c for c in raw.column_names if c != "EntityId"]), by=["EntityId"])
    return parse_instants(latest, sorted(instants))


trading_latest = trading_snapshots()


def trading_view(sm_type):
    return trading_latest.where(f"SmType == `{sm_type}`").view(
        ["EntityId", "State", "Version"] + [c for c, _, _ in TRADING[sm_type]["columns"]])


trading_orders = trading_view("trading_order")
trading_executions = trading_view("trading_execution")
trading_fills = trading_view("trading_fill")
trading_allocations = trading_view("trading_allocation")

# Slippage: aj = for each fill, the last tick of its symbol at or before the fill time. Side sign: BUY +1,
# SELL / SELL_SHORT -1, so positive bps = cost. Fills older than the ticks ring (~25 min) get no tick.
fill_slippage = trading_fills.where("!isNull(Ts)") \
    .aj(ticks, on=["Symbol", "Ts >= Ts"], joins=["Mid", "Bid", "Ask", "TickTs = Ts"]) \
    .update(["SideSign = Side == `BUY` ? 1 : -1",
             "SlippageBps = isNull(Mid) ? NULL_DOUBLE : (Price - Mid) / Mid * 10000 * SideSign",
             "TickAgeMs = isNull(TickTs) ? NULL_LONG : diffMillis(TickTs, Ts)"]) \
    .view(["FillId", "OrderId", "AccountId", "Symbol", "Side", "Venue", "Liquidity", "Quantity", "Price",
           "Bid", "Ask", "Mid", "SlippageBps", "Fee", "Ts", "TickAgeMs"])
slippage_by_venue = fill_slippage.where("!isNull(SlippageBps)").update("QtyBps = SlippageBps * Quantity") \
    .agg_by([agg.count_("Fills"), agg.sum_(["Shares = Quantity", "QtyBps", "Fees = Fee"]),
             agg.avg(["AvgSlippageBps = SlippageBps"])], by=["Venue", "Liquidity"]) \
    .update("QtyWeightedBps = QtyBps / Shares").drop_columns(["QtyBps"]).sort(["Venue", "Liquidity"])

# Implementation shortfall (avg px vs arrival mid) and avg px vs the live market VWAP of the symbol.
vwap_vs_arrival = trading_orders.where(["FilledQty > 0", "!isNull(AvgPx)", "!isNull(ArrivalPx)"]) \
    .natural_join(vwap_by_symbol, on=["Symbol"], joins=["MarketVwap = Vwap"]) \
    .update(["SideSign = Side == `BUY` ? 1 : -1",
             "ShortfallBps = (AvgPx - ArrivalPx) / ArrivalPx * 10000 * SideSign",
             "VsMarketVwapBps = isNull(MarketVwap) ? NULL_DOUBLE : (AvgPx - MarketVwap) / MarketVwap * 10000 * SideSign"]) \
    .view(["OrderId", "AccountId", "Symbol", "Side", "OrderType", "State", "Quantity", "FilledQty", "ArrivalPx", "AvgPx",
           "ShortfallBps", "MarketVwap", "VsMarketVwapBps", "CreatedAt", "CompletedAt"])
fill_vwap_by_symbol = trading_fills.update("Notional = Price * Quantity") \
    .agg_by([agg.count_("Fills"), agg.sum_(["Notional", "Shares = Quantity"])], by=["Symbol"]) \
    .update("FillVwap = Notional / Shares") \
    .natural_join(vwap_by_symbol, on=["Symbol"], joins=["MarketVwap = Vwap"]) \
    .update("FillVsMarketBps = isNull(MarketVwap) ? NULL_DOUBLE : (FillVwap - MarketVwap) / MarketVwap * 10000") \
    .sort_descending(["Shares"])

# Positions from allocations (net quantity and cost per account and symbol; accounts start flat), marked to
# the live mid: account_pnl ticks with the market.
account_positions = trading_allocations \
    .update(["SignedQty = Side == `BUY` ? Quantity : -Quantity", "SignedCost = SignedQty * AvgPx"]) \
    .agg_by([agg.count_("Allocations"), agg.sum_(["NetQty = SignedQty", "NetCost = SignedCost"])],
            by=["AccountId", "Symbol"]) \
    .update("AvgCost = NetQty == 0 ? NULL_DOUBLE : NetCost / NetQty")
account_pnl = account_positions.natural_join(quotes_latest, on=["Symbol"], joins=["Mid", "QuoteTs = Ts"]) \
    .update(["MarketValue = isNull(Mid) ? NULL_DOUBLE : NetQty * Mid",
             "UnrealizedPnl = isNull(Mid) ? NULL_DOUBLE : NetQty * Mid - NetCost"]) \
    .sort(["AccountId", "Symbol"])
pnl_by_account = account_pnl.agg_by([agg.count_("Symbols"), agg.sum_(["NetQty", "MarketValue", "UnrealizedPnl"])],
                                    by=["AccountId"]).sort_descending(["UnrealizedPnl"])

# Traded notional by sector and side (fills x instruments), and live P&L by sector.
sector_exposure = trading_fills.natural_join(instruments, on=["Symbol"], joins=["Sector"]) \
    .update(["Notional = Price * Quantity", "SignedNotional = Side == `BUY` ? Notional : -Notional"]) \
    .agg_by([agg.count_("Fills"), agg.sum_(["Shares = Quantity", "Notional", "SignedNotional"])], by=["Sector", "Side"]) \
    .sort_descending(["Notional"])
sector_pnl = account_pnl.natural_join(instruments, on=["Symbol"], joins=["Sector"]) \
    .agg_by([agg.sum_(["MarketValue", "UnrealizedPnl"])], by=["Sector"]).sort_descending(["UnrealizedPnl"])

# ------------------------------------------------------------------ generated ecosystems
# ./generate-concert-ecosystem writes app.d/ecosystems/<name>.py per ecosystem. Each file runs with this script's
# globals (consume, parse_instants, TYPES, agg, kc, dht) and names its tables in ECOSYSTEM_FIELDS; they are exported
# like the tables above and become console globals. Only deployed ecosystems are loaded: their compose override
# (showcases/<name>/compose.yml) sets CONCERT_ECOSYSTEM_<NAME>=on here. A file that fails is logged and skipped.
ECOSYSTEM_DIR = os.path.join(HERE, "ecosystems")
ECOSYSTEM_TABLES = {}
for _eco_file in (sorted(os.listdir(ECOSYSTEM_DIR)) if os.path.isdir(ECOSYSTEM_DIR) else []):
    if not _eco_file.endswith(".py"):
        continue
    if not os.environ.get("CONCERT_ECOSYSTEM_" + _eco_file[:-3].upper().replace("-", "_")):
        continue
    _eco_path = os.path.join(ECOSYSTEM_DIR, _eco_file)
    _eco_scope = dict(globals())
    try:
        with open(_eco_path) as _f:
            exec(compile(_f.read(), _eco_path, "exec"), _eco_scope)
        _eco_fields = _eco_scope.get("ECOSYSTEM_FIELDS", {})
        ECOSYSTEM_TABLES.update(_eco_fields)
        globals().update(_eco_fields)
        print(f"[concert] ecosystem {_eco_file[:-3]}: {', '.join(_eco_fields)}")
    except Exception as _e:  # keep the rest of the app up
        print(f"[concert] ecosystem {_eco_file} failed: {_e!r}")

# ------------------------------------------------------------------ export as application fields
# (widgets for /iframe/widget/?name=...; the same objects are console globals)
APP_FIELDS = {
    "snapshots_raw": snapshots_raw, "snapshots": snapshots, "delivered_orders": delivered_orders,
    "orders_latest": orders_latest, "payments_latest": payments_latest, "shipments_latest": shipments_latest,
    "completions_per_minute": completions_per_minute, "state_distribution": state_distribution,
    "completion_latency": completion_latency, "revenue_by_currency": revenue_by_currency,
    "revenue_by_customer": revenue_by_customer,
    "instruments": instruments, "accounts": accounts, "venues": venues,
    "ticks": ticks, "quotes_latest": quotes_latest, "bars_1m": bars_1m,
    "vwap_by_symbol": vwap_by_symbol, "quotes_with_sector": quotes_with_sector, "sector_moves": sector_moves,
    "trading_orders": trading_orders, "trading_executions": trading_executions, "trading_fills": trading_fills,
    "trading_allocations": trading_allocations, "fill_slippage": fill_slippage, "slippage_by_venue": slippage_by_venue,
    "vwap_vs_arrival": vwap_vs_arrival, "fill_vwap_by_symbol": fill_vwap_by_symbol,
    "account_positions": account_positions, "account_pnl": account_pnl, "pnl_by_account": pnl_by_account,
    "sector_exposure": sector_exposure, "sector_pnl": sector_pnl,
    **ECOSYSTEM_TABLES,
}


def _export(app: ApplicationState):
    for name, table in APP_FIELDS.items():
        app[name] = table


_export(get_app_state())
del _export
print(f"[concert] Deephaven app ready: {', '.join(APP_FIELDS)}")
