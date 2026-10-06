# Fill slippage with an as-of join (aj): each fill is matched to the latest tick of its symbol at or
# before the fill time, and priced against that tick's mid.
#
# trading_fills (completed trading_fill entities from entity-snapshots, latest version per fill) and
# fill_slippage come from the concert app; run the trading showcase (scripts/up.sh <store> --analytics
# --trading, then scripts/trading-load.sh) to fill them. This notebook rebuilds the aj step by step and
# adds per-symbol and per-liquidity breakdowns plus a time-bucketed view.
from deephaven import agg
from deephaven.plot.figure import Figure

print(f"{trading_fills.size} fills so far; slippage rows: {fill_slippage.size}")

# The aj itself (same as the app's fill_slippage): stamp column Ts on both sides, exact match on Symbol.
slippage = (trading_fills
            .aj(ticks, on=["Symbol", "Ts >= Ts"], joins=["MidAtFill = Mid", "BidAtFill = Bid", "AskAtFill = Ask"])
            .update(["SideSign = Side == `BUY` ? 1 : -1",
                     "SlippageBps = isNull(MidAtFill) ? NULL_DOUBLE : (Price - MidAtFill) / MidAtFill * 10000 * SideSign",
                     "HalfSpreadBps = isNull(MidAtFill) ? NULL_DOUBLE : (AskAtFill - BidAtFill) / 2 / MidAtFill * 10000"]))

# Aggressive (TAKER) fills pay about half the spread, passive (MAKER) fills earn it.
slippage_by_liquidity = slippage.where("!isNull(SlippageBps)").agg_by(
    [agg.count_("Fills"), agg.avg(["AvgSlippageBps = SlippageBps", "AvgHalfSpreadBps = HalfSpreadBps"]),
     agg.sum_(["Fees = Fee"])], by=["Liquidity", "Side"])

slippage_by_symbol = slippage.where("!isNull(SlippageBps)").agg_by(
    [agg.count_("Fills"), agg.sum_(["Shares = Quantity"]), agg.avg(["AvgSlippageBps = SlippageBps"])],
    by=["Symbol"]).sort_descending(["Shares"])

# Average slippage per 10-second bucket, as a line chart.
slippage_10s = (slippage.where("!isNull(SlippageBps)")
                .update("Bucket = lowerBin(Ts, 10 * SECOND)")
                .agg_by([agg.avg(["AvgSlippageBps = SlippageBps"]), agg.count_("Fills")], by=["Bucket"])
                .sort("Bucket"))
slippage_plot = (Figure()
                 .plot_xy(series_name="avg slippage (bps)", t=slippage_10s, x="Bucket", y="AvgSlippageBps")
                 .chart_title(title="Fill slippage vs mid at fill time")
                 .show())
