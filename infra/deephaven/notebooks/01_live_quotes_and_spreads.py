# Live quotes and spreads.
# Uses tables the concert app (app.d/concert.py) already put in this console: ticks, quotes_latest,
# quotes_with_sector, instruments. Everything below ticks along as marketdata-sim publishes.
from deephaven import agg
from deephaven.plot.figure import Figure

# Spread in basis points of the mid, widest first.
spreads = quotes_with_sector.update("SpreadBps = (Ask - Bid) / Mid * 10000").sort_descending("SpreadBps")

# Average / widest spread per sector.
spread_by_sector = spreads.agg_by(
    [agg.count_("Symbols"), agg.avg("AvgSpreadBps = SpreadBps"), agg.max_("MaxSpreadBps = SpreadBps")], by="Sector")

# Top 3 movers (vs the instrument's reference price) in each sector.
top_movers = (quotes_with_sector.update("AbsMove = abs(MoveVsRefPct)")
              .sort_descending("AbsMove").head_by(3, "Sector").drop_columns("AbsMove"))

# A ticking plot: bid / mid / ask of one symbol over the ticks ring buffer (last ~25 min).
SYMBOL = "AAPL"
one = ticks.where(f"Symbol == `{SYMBOL}`")
quote_plot = (Figure()
              .plot_xy(series_name="bid", t=one, x="Ts", y="Bid")
              .plot_xy(series_name="mid", t=one, x="Ts", y="Mid")
              .plot_xy(series_name="ask", t=one, x="Ts", y="Ask")
              .chart_title(title=f"{SYMBOL} top of book")
              .show())
