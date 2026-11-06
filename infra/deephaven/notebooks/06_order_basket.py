# A basket of randomized orders marked to market: build your own (size, seed, sides) and watch its market value tick.
# The concert app already has one (basket_orders / basket_positions / basket_value / basket_value_chart, BASKET_SEED
# env); this notebook shows how it is built, with a truly random basket you can re-roll by re-running the cell.
#
# Needs market data: scripts/up.sh <store> --analytics (marketdata-sim; start / stop it from a flow's Deephaven page).
import random

from deephaven import agg, new_table, time_table
from deephaven.column import int_col, string_col
from deephaven.pandas import to_pandas
from deephaven.plot.figure import Figure

N = 15                      # orders in the basket
symbols = sorted(to_pandas(instruments.view(["Symbol"]))["Symbol"].tolist())  # the universe right now
if not symbols:
    raise RuntimeError("no instruments yet: is marketdata-sim running? (scripts/up.sh <store> --analytics)")
picked = random.sample(symbols, min(N, len(symbols)))

my_basket = new_table([
    string_col("Symbol", picked),
    string_col("Side", [random.choice(["BUY", "BUY", "SELL"]) for _ in picked]),
    int_col("Quantity", [random.randint(1, 20) * 100 for _ in picked]),
])

# Every tick of a basket symbol re-marks its order.
my_positions = (my_basket
                .natural_join(quotes_latest, on=["Symbol"], joins=["Mid", "QuoteTs = Ts"])
                .natural_join(instruments, on=["Symbol"], joins=["Name", "Sector", "RefPrice"])
                .update(["SignedQty = Side == `BUY` ? Quantity : -Quantity",
                         "MarketValue = SignedQty * Mid",
                         "PnlVsRef = SignedQty * (Mid - RefPrice)"]))

my_value = my_positions.agg_by([agg.count_("Orders"), agg.sum_(["NetMarketValue = MarketValue", "PnlVsRef"]),
                                agg.max_(["AsOf = QuoteTs"])])
my_history = my_value.snapshot_when(time_table("PT1s"), history=True)
my_chart = (Figure()
            .plot_xy(series_name="net market value", t=my_history, x="Timestamp", y="NetMarketValue")
            .chart_title(title="My basket, marked to the live mid")
            .show())
