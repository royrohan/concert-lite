package io.concert.sink.clickhouse;

import io.concert.sink.SchemaSpec;
import io.concert.sink.TableSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Trading analytics views: the concert path (completed {@code trading_*} entities, root tables from
 * {@link ClickHouseSchema}) joined with the market-data side path ({@code ticks}, {@code bars_1m},
 * {@code instruments} from {@link MarketDataSchema}). Generated only when the schema has the trading
 * roots (smTypes {@code trading_order}, {@code trading_fill}, {@code trading_allocation}) and market
 * data is provisioned; each view names its tables via the schema, so table renames follow.
 *
 * <pre>
 * trading_fill_slippage      fills ASOF JOIN ticks (same symbol, last tick at or before the fill): mid, bid, ask
 *                            at fill time, slippage in bps signed by side (positive = cost)
 * trading_sector_exposure    fills ⨝ instruments FINAL: fills, shares, notional by sector and side
 * trading_vwap_vs_arrival    per order: avg_px vs arrival_px (implementation shortfall, bps) and vs the market
 *                            VWAP of the bars_1m minutes the order was live in
 * trading_symbol_vwap        per symbol: VWAP of our fills vs the market VWAP of bars_1m over the same minutes
 * trading_account_positions  allocations per account and symbol (net qty, cost, avg px) ⨝ latest quote:
 *                            market value and unrealized P&amp;L (positions start flat each run)
 * </pre>
 *
 * Side sign: {@code BUY} is +1, {@code SELL} / {@code SELL_SHORT} -1. All views read the root tables
 * with {@code FINAL} (one row per entity).
 */
public final class TradingAnalytics {

    public static final String ORDER = "trading_order";
    public static final String FILL = "trading_fill";
    public static final String ALLOCATION = "trading_allocation";

    public static final String SLIPPAGE = "trading_fill_slippage";
    public static final String SECTOR_EXPOSURE = "trading_sector_exposure";
    public static final String VWAP_VS_ARRIVAL = "trading_vwap_vs_arrival";
    public static final String SYMBOL_VWAP = "trading_symbol_vwap";
    public static final String ACCOUNT_POSITIONS = "trading_account_positions";

    private static final String SIGN = "if(side = 'BUY', 1, -1)";

    private TradingAnalytics() {}

    /** The views, for the roots present in {@code schema} (none without market data). */
    public static List<Ddl> views(SchemaSpec schema, boolean marketData) {
        List<Ddl> out = new ArrayList<>();
        if (!marketData) {
            return out;
        }
        Optional<String> fills = table(schema, FILL);
        Optional<String> orders = table(schema, ORDER);
        Optional<String> allocations = table(schema, ALLOCATION);
        fills.ifPresent(f -> {
            out.add(view(SLIPPAGE, slippage(f)));
            out.add(view(SECTOR_EXPOSURE, sectorExposure(f)));
            out.add(view(SYMBOL_VWAP, symbolVwap(f)));
        });
        orders.ifPresent(o -> out.add(view(VWAP_VS_ARRIVAL, vwapVsArrival(o))));
        allocations.ifPresent(a -> out.add(view(ACCOUNT_POSITIONS, accountPositions(a))));
        return out;
    }

    /** Sample queries over the views, for the trace UI's ClickHouse console. */
    public static List<ClickHouseSamples.Sample> samples(SchemaSpec schema, boolean marketData) {
        List<ClickHouseSamples.Sample> out = new ArrayList<>();
        if (!marketData) {
            return out;
        }
        if (table(schema, FILL).isPresent()) {
            out.add(new ClickHouseSamples.Sample("Trading: fill slippage vs mid at fill time (ASOF JOIN ticks)",
                    "SELECT * FROM " + SLIPPAGE + "\nORDER BY ts DESC\nLIMIT 100"));
            out.add(new ClickHouseSamples.Sample("Trading: slippage by venue and liquidity",
                    "SELECT venue, liquidity, count() AS fills, sum(quantity) AS shares,\n"
                            + "       round(avg(slippage_bps), 3) AS avg_slippage_bps,\n"
                            + "       round(sum(slippage_bps * quantity) / sum(quantity), 3) AS qty_weighted_bps,\n"
                            + "       round(sum(toFloat64(fee)), 2) AS fees\nFROM " + SLIPPAGE
                            + "\nGROUP BY venue, liquidity\nORDER BY avg_slippage_bps"));
            out.add(new ClickHouseSamples.Sample("Trading: notional by sector and side (fills ⨝ instruments)",
                    "SELECT * FROM " + SECTOR_EXPOSURE));
            out.add(new ClickHouseSamples.Sample("Trading: fill VWAP vs market VWAP per symbol (bars_1m)",
                    "SELECT * FROM " + SYMBOL_VWAP));
        }
        if (table(schema, ORDER).isPresent()) {
            out.add(new ClickHouseSamples.Sample("Trading: implementation shortfall per order (avg px vs arrival)",
                    "SELECT * FROM " + VWAP_VS_ARRIVAL + "\nORDER BY completed_at DESC\nLIMIT 100"));
            out.add(new ClickHouseSamples.Sample("Trading: shortfall by side and order type",
                    "SELECT side, order_type, count() AS orders, round(avg(shortfall_bps), 3) AS avg_shortfall_bps,\n"
                            + "       round(avg(vs_market_vwap_bps), 3) AS avg_vs_market_vwap_bps\nFROM " + VWAP_VS_ARRIVAL
                            + "\nGROUP BY side, order_type\nORDER BY side, order_type"));
        }
        if (table(schema, ALLOCATION).isPresent()) {
            out.add(new ClickHouseSamples.Sample("Trading: account positions with unrealized P&L (latest quote)",
                    "SELECT * FROM " + ACCOUNT_POSITIONS + "\nORDER BY abs(unrealized_pnl) DESC\nLIMIT 100"));
            out.add(new ClickHouseSamples.Sample("Trading: P&L per account",
                    "SELECT account_id, count() AS symbols, sum(net_qty) AS net_shares,\n"
                            + "       round(sum(market_value), 2) AS market_value, round(sum(unrealized_pnl), 2) AS unrealized_pnl\n"
                            + "FROM " + ACCOUNT_POSITIONS + "\nGROUP BY account_id\nORDER BY unrealized_pnl DESC"));
        }
        return out;
    }

    private static Optional<String> table(SchemaSpec schema, String smType) {
        return schema.rootFor(smType).map(TableSpec::name);
    }

    private static Ddl.View view(String name, String select) {
        return new Ddl.View(name, select, "analytics trading " + name);
    }

    static String slippage(String fills) {
        return "WITH f AS (\n"
                + "    SELECT fill_id, order_id, exec_id, account_id, assumeNotNull(symbol) AS symbol, side, venue,\n"
                + "           liquidity, quantity, price, fee, assumeNotNull(ts) AS ts\n"
                + "    FROM " + Sql.id(fills) + " FINAL\n"
                + "    WHERE ts IS NOT NULL AND symbol IS NOT NULL\n"
                + ")\n"
                + "SELECT f.fill_id AS fill_id, f.order_id AS order_id, f.exec_id AS exec_id, f.account_id AS account_id,\n"
                + "       f.symbol AS symbol, f.side AS side, f.venue AS venue, f.liquidity AS liquidity, f.quantity AS quantity,\n"
                + "       f.price AS price, t.bid AS bid_at_fill, t.ask AS ask_at_fill, t.mid AS mid_at_fill,\n"
                + "       round(toFloat64(f.price - t.mid) / toFloat64(t.mid) * 10000 * if(f.side = 'BUY', 1, -1), 3) AS slippage_bps,\n"
                + "       f.fee AS fee, f.ts AS ts, t.ts AS tick_ts, dateDiff('millisecond', t.ts, f.ts) AS tick_age_ms\n"
                + "FROM f\n"
                // only the ticks the fills can match: their symbols, from a minute before the first fill
                + "ASOF JOIN (\n"
                + "    SELECT toString(symbol) AS symbol, ts, bid, ask, mid FROM ticks\n"
                + "    WHERE ts >= (SELECT min(ts) - INTERVAL 1 MINUTE FROM " + Sql.id(fills) + ")\n"
                + "      AND toString(symbol) IN (SELECT DISTINCT assumeNotNull(symbol) FROM " + Sql.id(fills) + ")\n"
                + ") AS t ON t.symbol = f.symbol AND f.ts >= t.ts";
    }

    static String sectorExposure(String fills) {
        return "SELECT i.sector AS sector, f.side AS side, count() AS fills, uniqExact(f.order_id) AS orders,\n"
                + "       sum(f.quantity) AS shares, round(sum(toFloat64(f.price) * f.quantity), 2) AS notional,\n"
                + "       round(sum(toFloat64(f.price) * f.quantity * if(f.side = 'BUY', 1, -1)), 2) AS signed_notional,\n"
                + "       round(sum(toFloat64(f.fee)), 2) AS fees\n"
                + "FROM " + Sql.id(fills) + " AS f FINAL\n"
                + "INNER JOIN instruments AS i FINAL ON i.symbol = assumeNotNull(f.symbol)\n"
                + "GROUP BY sector, side\nORDER BY notional DESC";
    }

    static String vwapVsArrival(String orders) {
        return "WITH o AS (\n"
                + "    SELECT order_id, account_id, assumeNotNull(symbol) AS symbol, side, order_type, sm_state,\n"
                + "           quantity, filled_qty, toFloat64(arrival_px) AS arrival_px, toFloat64(avg_px) AS avg_px,\n"
                + "           created_at, completed_at\n"
                + "    FROM " + Sql.id(orders) + " FINAL\n"
                + "    WHERE filled_qty > 0 AND avg_px IS NOT NULL AND arrival_px IS NOT NULL\n"
                + "),\n"
                + "m AS (\n"
                + "    SELECT o.order_id AS order_id,\n"
                + "           sum(toFloat64(b.vwap) * b.volume) / nullIf(sum(b.volume), 0) AS market_vwap\n"
                + "    FROM o INNER JOIN bars_1m AS b ON b.symbol = o.symbol\n"
                + "    WHERE b.vwap IS NOT NULL AND b.start >= toStartOfMinute(assumeNotNull(o.created_at))\n"
                + "      AND b.start <= assumeNotNull(o.completed_at)\n"
                + "    GROUP BY o.order_id\n"
                + ")\n"
                + "SELECT o.order_id AS order_id, o.account_id AS account_id, o.symbol AS symbol, o.side AS side,\n"
                + "       o.order_type AS order_type, o.sm_state AS sm_state, o.quantity AS quantity, o.filled_qty AS filled_qty,\n"
                + "       o.arrival_px AS arrival_px, o.avg_px AS avg_px,\n"
                + "       round((o.avg_px - o.arrival_px) / o.arrival_px * 10000 * if(o.side = 'BUY', 1, -1), 3) AS shortfall_bps,\n"
                + "       round(m.market_vwap, 4) AS market_vwap,\n"
                + "       round((o.avg_px - m.market_vwap) / m.market_vwap * 10000 * if(o.side = 'BUY', 1, -1), 3) AS vs_market_vwap_bps,\n"
                + "       o.created_at AS created_at, o.completed_at AS completed_at\n"
                + "FROM o LEFT JOIN m ON m.order_id = o.order_id";
    }

    static String symbolVwap(String fills) {
        return "WITH f AS (\n"
                + "    SELECT assumeNotNull(symbol) AS symbol, count() AS fills, sum(quantity) AS shares,\n"
                + "           sum(toFloat64(price) * quantity) / sum(quantity) AS fill_vwap,\n"
                + "           min(toStartOfMinute(assumeNotNull(ts))) AS first_minute, max(assumeNotNull(ts)) AS last_ts\n"
                + "    FROM " + Sql.id(fills) + " FINAL\n"
                + "    WHERE ts IS NOT NULL AND symbol IS NOT NULL AND quantity > 0\n"
                + "    GROUP BY symbol\n"
                + "),\n"
                + "m AS (\n"
                + "    SELECT f.symbol AS symbol, sum(toFloat64(b.vwap) * b.volume) / nullIf(sum(b.volume), 0) AS market_vwap,\n"
                + "           sum(b.volume) AS market_volume\n"
                + "    FROM f INNER JOIN bars_1m AS b ON b.symbol = f.symbol\n"
                + "    WHERE b.vwap IS NOT NULL AND b.start >= f.first_minute AND b.start <= f.last_ts\n"
                + "    GROUP BY f.symbol\n"
                + ")\n"
                + "SELECT f.symbol AS symbol, f.fills AS fills, f.shares AS shares, round(f.fill_vwap, 4) AS fill_vwap,\n"
                + "       round(m.market_vwap, 4) AS market_vwap, m.market_volume AS market_volume,\n"
                + "       round((f.fill_vwap - m.market_vwap) / m.market_vwap * 10000, 3) AS fill_vs_market_bps\n"
                + "FROM f LEFT JOIN m ON m.symbol = f.symbol\nORDER BY f.shares DESC";
    }

    static String accountPositions(String allocations) {
        return "WITH p AS (\n"
                + "    SELECT account_id, assumeNotNull(symbol) AS symbol, count() AS allocations,\n"
                + "           sumIf(assumeNotNull(quantity), side = 'BUY') AS bought,\n"
                + "           sumIf(assumeNotNull(quantity), side != 'BUY') AS sold,\n"
                + "           sum(quantity * " + SIGN + ") AS net_qty,\n"
                + "           sum(toFloat64(avg_px) * quantity * " + SIGN + ") AS net_cost\n"
                + "    FROM " + Sql.id(allocations) + " FINAL\n"
                + "    WHERE symbol IS NOT NULL AND quantity > 0\n"
                + "    GROUP BY account_id, symbol\n"
                + "),\n"
                + "q AS (\n"
                + "    SELECT toString(symbol) AS symbol, argMax(mid, ts) AS mid, max(ts) AS quote_ts FROM ticks\n"
                + "    WHERE ts > now64(3) - INTERVAL 1 HOUR\n"
                + "    GROUP BY symbol\n"
                + ")\n"
                + "SELECT p.account_id AS account_id, p.symbol AS symbol, p.allocations AS allocations, p.bought AS bought,\n"
                + "       p.sold AS sold, p.net_qty AS net_qty, round(p.net_cost, 2) AS net_cost,\n"
                + "       if(p.net_qty = 0, NULL, round(p.net_cost / p.net_qty, 4)) AS avg_px,\n"
                + "       q.mid AS mid, q.quote_ts AS quote_ts,\n"
                + "       round(p.net_qty * toFloat64(q.mid), 2) AS market_value,\n"
                + "       round(p.net_qty * toFloat64(q.mid) - p.net_cost, 2) AS unrealized_pnl\n"
                + "FROM p INNER JOIN q ON q.symbol = p.symbol";
    }
}
