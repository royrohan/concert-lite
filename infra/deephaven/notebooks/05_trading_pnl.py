# Live P&L per account: positions from completed allocations (the concert path), marked to the live mid
# (the market-data side path). account_positions / account_pnl / pnl_by_account / sector_pnl come from the
# concert app; this notebook shows how they are built and adds a P&L history that ticks with the market.
#
# Run the trading showcase first: scripts/up.sh <store> --analytics --trading, then scripts/trading-load.sh.
from deephaven import agg, time_table
from deephaven.plot.figure import Figure

print(f"{trading_allocations.size} allocations, {account_pnl.size} positions")

# Net position and cost per account and symbol (accounts start flat; SELL and SELL_SHORT reduce).
positions = (trading_allocations
             .update(["SignedQty = Side == `BUY` ? Quantity : -Quantity", "SignedCost = SignedQty * AvgPx"])
             .agg_by([agg.sum_(["NetQty = SignedQty", "NetCost = SignedCost"])], by=["AccountId", "Symbol"]))

# natural_join to the latest quote: every new tick of a held symbol re-marks its positions.
marked = (positions
          .natural_join(quotes_latest, on=["Symbol"], joins=["Mid"])
          .update(["MarketValue = NetQty * Mid", "UnrealizedPnl = MarketValue - NetCost"]))

# Desk view: accounts joined with their reference data.
pnl_by_desk = (marked.natural_join(accounts, on=["AccountId"], joins=["Desk", "AccountName = Name"])
               .agg_by([agg.sum_(["MarketValue", "UnrealizedPnl"]), agg.count_("Positions")], by=["Desk"])
               .sort_descending(["UnrealizedPnl"]))

# Snapshot the total P&L every second into a history table and chart it.
total_pnl = marked.agg_by([agg.sum_(["UnrealizedPnl", "MarketValue"])])
pnl_history = total_pnl.snapshot_when(time_table("PT1s"), history=True)  # adds the trigger's Timestamp
pnl_plot = (Figure()
            .plot_xy(series_name="total unrealized P&L", t=pnl_history, x="Timestamp", y="UnrealizedPnl")
            .chart_title(title="Unrealized P&L (allocations x live mid)")
            .show())

# Top movers: biggest absolute P&L positions right now.
top_positions = marked.update("AbsPnl = abs(UnrealizedPnl)").sort_descending(["AbsPnl"]).head(20)
