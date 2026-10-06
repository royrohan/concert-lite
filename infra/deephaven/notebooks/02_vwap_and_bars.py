# VWAP and bars.
# vwap_by_symbol (VWAP over the ticks ring) and bars_1m (md.bars.1m) come from the concert app; this adds a
# rolling 1-minute VWAP per symbol, 5-minute bars rolled up from the 1-minute ones, and an OHLC chart.
from deephaven import agg
from deephaven.plot.figure import Figure
from deephaven.updateby import rolling_sum_time

# Rolling 1-minute VWAP on every trade tick (quote-only ticks have LastSize 0 or null).
trades = ticks.where("!isNull(LastSize) && LastSize > 0").update("Notional = Last * LastSize")
rolling_vwap = (trades
                .update_by([rolling_sum_time("Ts", ["Notional1m = Notional", "Qty1m = LastSize"], rev_time="PT1m")],
                           by="Symbol")
                .update("Vwap1m = Notional1m / Qty1m")
                .view(["Symbol", "Ts", "Last", "LastSize", "Vwap1m"]))
latest_rolling_vwap = rolling_vwap.last_by("Symbol")

# 5-minute bars from the 1-minute bars.
bars_5m = (bars_1m.update("Bucket = lowerBin(Start, 5 * MINUTE)")
           .sort(["Symbol", "Start"])
           .agg_by([agg.first("Open"), agg.max_("High"), agg.min_("Low"), agg.last("Close"),
                    agg.sum_(["Volume", "Trades"])], by=["Symbol", "Bucket"]))

# Candles for one symbol.
SYMBOL = "MSFT"
candles = bars_1m.where(f"Symbol == `{SYMBOL}`").sort("Start")
ohlc_plot = (Figure()
             .plot_ohlc(series_name=SYMBOL, t=candles, x="Start", open="Open", high="High", low="Low", close="Close")
             .chart_title(title=f"{SYMBOL} 1-minute bars")
             .show())
