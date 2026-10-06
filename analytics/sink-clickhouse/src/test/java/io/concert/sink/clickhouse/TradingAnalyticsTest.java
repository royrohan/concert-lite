package io.concert.sink.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.concert.sink.EntitySnapshot;
import io.concert.sink.SchemaSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The trading views ({@link TradingAnalytics}): generated per root and only with market data, and, with
 * {@code clickhouse local} (skipped without a binary or Docker), evaluated over hand-computed fills,
 * an order, allocations, ticks, bars and instruments.
 */
class TradingAnalyticsTest {

    @Test
    void viewsFollowTheRootsAndNeedMarketData() {
        SchemaSpec schema = Fixtures.tradingSchema();
        assertEquals(List.of(TradingAnalytics.SLIPPAGE, TradingAnalytics.SECTOR_EXPOSURE, TradingAnalytics.SYMBOL_VWAP,
                TradingAnalytics.VWAP_VS_ARRIVAL, TradingAnalytics.ACCOUNT_POSITIONS),
                TradingAnalytics.views(schema, true).stream().map(Ddl::name).toList());
        assertTrue(TradingAnalytics.views(schema, false).isEmpty());
        assertTrue(TradingAnalytics.views(Fixtures.schema(), true).isEmpty()); // no trading roots
        assertEquals(8, TradingAnalytics.samples(schema, true).size());
        assertTrue(TradingAnalytics.slippage("trading_fills").contains("ASOF JOIN"));
    }

    private static EntitySnapshot fill(String id, String symbol, String side, int qty, String price, Instant ts) {
        String json = "{\"accountId\":\"ACC-1\",\"execId\":\"E1\",\"fee\":0.05,\"fillId\":\"" + id + "\",\"liquidity\":\"TAKER\","
                + "\"orderId\":\"O1\",\"price\":" + price + ",\"quantity\":" + qty + ",\"side\":\"" + side + "\",\"status\":\"BOOKED\","
                + "\"symbol\":\"" + symbol + "\",\"ts\":\"" + ts + "\",\"venue\":\"XNAS\"}";
        return new EntitySnapshot("trading_fill:" + id, "trading_fill", "BOOKED", 1, ts, ts, json);
    }

    private static EntitySnapshot allocation(String id, String account, String symbol, String side, int qty, String px, Instant ts) {
        String json = "{\"accountId\":\"" + account + "\",\"allocId\":\"" + id + "\",\"avgPx\":" + px + ",\"orderId\":\"O1\","
                + "\"quantity\":" + qty + ",\"side\":\"" + side + "\",\"status\":\"CONFIRMED\",\"symbol\":\"" + symbol + "\"}";
        return new EntitySnapshot("trading_allocation:" + id, "trading_allocation", "CONFIRMED", 2, ts, ts, json);
    }

    /** Like {@link ClickHouseLocalDdlTest#insertRaw}, with the timestamps in ClickHouse's basic text format. */
    private static String insertRaw(EntitySnapshot s) {
        return "INSERT INTO entity_snapshots (entity_id, entity_version, sm_type, sm_state, created_at, completed_at, model_json,"
                + " kafka_partition, kafka_offset) VALUES (" + Sql.str(s.entityId()) + ", " + s.version() + ", " + Sql.str(s.smType())
                + ", " + Sql.str(s.state()) + ", " + Sql.str(text(s.createdAt())) + ", " + Sql.str(text(s.completedAt()))
                + ", " + Sql.str(s.modelJson()) + ", 0, 0)";
    }

    private static String text(Instant t) {
        return t.toString().replace("T", " ").replace("Z", "");
    }

    private static String tick(String symbol, Instant ts, String bid, String ask, String mid) {
        return "INSERT INTO ticks (symbol, ts, ts_millis, seq, bid, bid_size, ask, ask_size, mid, last, last_size, volume) VALUES ("
                + Sql.str(symbol) + ", " + Sql.str(text(ts)) + ", " + ts.toEpochMilli()
                + ", 1, " + bid + ", 100, " + ask + ", 100, " + mid + ", " + mid + ", 0, 0)";
    }

    @Test
    void viewsComputeSlippageExposureShortfallAndPnl() throws Exception {
        List<String> cmd = ClickHouseLocalDdlTest.command();
        assumeTrue(cmd != null, "neither a clickhouse binary nor Docker with " + ClickHouseLocalDdlTest.IMAGE);
        // recent times (ticks have a TTL), 10 s into a minute so the order's bar minute is unambiguous
        Instant base = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(10, ChronoUnit.MINUTES).plusSeconds(10);
        List<String> stmts = new ArrayList<>();
        for (Ddl d : Fixtures.allWithTrading("kafka:9092")) {
            switch (d) {
                case Ddl.Table t -> stmts.add(t.create());
                case Ddl.View v -> stmts.add(v.create());
                case Ddl.MaterializedView m -> stmts.add(m.create());
                case Ddl.Queue q -> stmts.add(q.body().substring(0, q.body().indexOf("\n)\nENGINE"))
                        + ",\n    _topic String, _partition UInt64, _offset UInt64, _error String, _raw_message String\n) ENGINE = Memory");
            }
        }
        stmts.add("INSERT INTO instruments (symbol, isin, name, asset_class, sector, currency, lot_size, tick_size, ref_price,"
                + " primary_mic, kafka_offset) VALUES ('AAPL', 'US0378331005', 'Apple', 'EQUITY', 'TECHNOLOGY', 'USD', 100, 0.01, 100,"
                + " 'XNAS', 1), ('XOM', 'US30231G1022', 'Exxon', 'EQUITY', 'ENERGY', 'USD', 100, 0.01, 50, 'XNYS', 2)");
        stmts.add(tick("AAPL", base, "99.99", "100.01", "100.00"));
        stmts.add(tick("AAPL", base.plusSeconds(1), "100.09", "100.11", "100.10"));
        stmts.add(tick("XOM", base, "49.99", "50.01", "50.00"));
        Instant now = Instant.now().minusSeconds(1);
        stmts.add(tick("AAPL", now, "100.99", "101.01", "101.00"));
        stmts.add(tick("XOM", now, "48.99", "49.01", "49.00"));
        String minute = Sql.str(text(base.truncatedTo(ChronoUnit.MINUTES)));
        stmts.add("INSERT INTO bars_1m (symbol, start, end, open, high, low, close, volume, vwap, trades, ticks) VALUES ('AAPL', "
                + minute + ", " + minute + " , 100, 100.2, 99.9, 100.1, 1000, 100.05, 10, 50)");
        stmts.add(insertRaw(fill("F1", "AAPL", "BUY", 100, "100.01", base.plusMillis(500))));
        stmts.add(insertRaw(fill("F2", "AAPL", "SELL", 40, "100.09", base.plusMillis(1200))));
        stmts.add(insertRaw(fill("F3", "XOM", "SELL", 50, "49.99", base.plusMillis(300))));
        stmts.add(insertRaw(new EntitySnapshot("trading_order:O1", "trading_order", "FILLED", 9, base,
                base.plusSeconds(2), "{\"accountId\":\"ACC-1\",\"allocations\":[],\"arrivalPx\":100.00,\"avgPx\":100.01,"
                        + "\"executions\":[],\"filledQty\":100,\"orderId\":\"O1\",\"orderType\":\"MARKET\",\"quantity\":100,"
                        + "\"side\":\"BUY\",\"status\":\"FILLED\",\"symbol\":\"AAPL\"}")));
        stmts.add(insertRaw(allocation("A1", "ACC-1", "AAPL", "BUY", 100, "100.01", base)));
        stmts.add(insertRaw(allocation("A2", "ACC-1", "AAPL", "SELL", 40, "100.09", base)));
        stmts.add(insertRaw(allocation("A3", "ACC-2", "XOM", "SELL", 50, "49.99", base)));
        stmts.add("SELECT fill_id, symbol, side, mid_at_fill, slippage_bps FROM trading_fill_slippage ORDER BY fill_id FORMAT TSV");
        stmts.add("SELECT sector, side, fills, shares, notional FROM trading_sector_exposure ORDER BY sector, side FORMAT TSV");
        stmts.add("SELECT order_id, shortfall_bps, market_vwap, vs_market_vwap_bps FROM trading_vwap_vs_arrival FORMAT TSV");
        stmts.add("SELECT symbol, fills, shares, fill_vwap, market_vwap FROM trading_symbol_vwap ORDER BY symbol FORMAT TSV");
        stmts.add("SELECT account_id, symbol, bought, sold, net_qty, net_cost, avg_px, mid, market_value, unrealized_pnl"
                + " FROM trading_account_positions ORDER BY account_id FORMAT TSV");
        String out = ClickHouseLocalDdlTest.run(cmd, String.join(";\n", stmts) + ";\n");
        assertEquals(List.of(
                "F1\tAAPL\tBUY\t100\t1",
                "F2\tAAPL\tSELL\t100.1\t0.999",
                "F3\tXOM\tSELL\t50\t2",
                "ENERGY\tSELL\t1\t50\t2499.5",
                "TECHNOLOGY\tBUY\t1\t100\t10001",
                "TECHNOLOGY\tSELL\t1\t40\t4003.6",
                "O1\t1\t100.05\t-3.998",
                "AAPL\t2\t140\t100.0329\t100.05",
                "XOM\t1\t50\t49.99\t\\N",
                "ACC-1\tAAPL\t100\t40\t60\t5997.4\t99.9567\t101\t6060\t62.6",
                "ACC-2\tXOM\t0\t50\t-50\t-2499.5\t49.99\t49\t-2450\t49.5"), out.lines().toList());
    }
}
