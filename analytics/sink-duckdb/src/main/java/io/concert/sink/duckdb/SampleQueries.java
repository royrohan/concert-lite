package io.concert.sink.duckdb;

import io.concert.sink.ColumnSpec;
import io.concert.sink.ColumnType;
import io.concert.sink.SchemaSpec;
import io.concert.sink.TableSpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Example queries for the trace UI, generated from the schema so they follow the models. */
public final class SampleQueries {

    public record Sample(String title, String sql) {}

    private SampleQueries() {}

    public static List<Sample> generate(SchemaSpec schema) {
        List<Sample> out = new ArrayList<>();
        for (TableSpec root : schema.roots()) {
            String t = DuckDbTarget.q(root.name());
            String preview = root.modelColumns().stream()
                    .filter(c -> c.type() != ColumnType.JSON).limit(6).map(c -> DuckDbTarget.q(c.name()))
                    .collect(Collectors.joining(", "));
            out.add(new Sample("Completed " + root.smType() + " entities",
                    "SELECT entity_id, sm_state, entity_version, created_at, completed_at"
                            + (preview.isEmpty() ? "" : ", " + preview) + "\nFROM " + t + "\nORDER BY completed_at DESC\nLIMIT 100"));
            out.add(new Sample(cap(root.smType()) + " count and time to complete by state",
                    "SELECT sm_state, count(*) AS n,\n       round(avg(date_diff('millisecond', created_at, completed_at)) / 1000.0, 3) AS avg_seconds\n"
                            + "FROM " + t + "\nGROUP BY sm_state\nORDER BY n DESC"));
            for (Map.Entry<String, ColumnSpec[]> money : moneyGroups(root).entrySet()) {
                String amount = DuckDbTarget.q(money.getValue()[0].name());
                String currency = DuckDbTarget.q(money.getValue()[1].name());
                out.add(new Sample(cap(root.smType()) + " " + money.getKey() + " by currency",
                        "SELECT " + currency + " AS currency, sm_state, count(*) AS n, sum(" + amount + ") AS total, avg(" + amount
                                + ") AS average\nFROM " + t + "\nWHERE " + amount + " IS NOT NULL\nGROUP BY ALL\nORDER BY total DESC"));
                break; // the first Money-like group is enough
            }
            for (TableSpec child : schema.childrenOf(root)) {
                String c = DuckDbTarget.q(child.name());
                out.add(new Sample(cap(root.smType()) + " " + child.sourcePath().getLast() + " with their " + root.smType(),
                        "SELECT r.entity_id, r.sm_state, c.* EXCLUDE (entity_id, entity_version)\nFROM " + t + " r\nJOIN " + c
                                + " c ON c.entity_id = r.entity_id AND c.entity_version = r.entity_version\n"
                                + "ORDER BY r.completed_at DESC, c.idx\nLIMIT 200"));
                out.add(new Sample(cap(child.sourcePath().getLast()) + " per " + root.smType(),
                        "SELECT r.sm_state, count(DISTINCT r.entity_id) AS entities, count(c.idx) AS rows,\n"
                                + "       round(count(c.idx) / count(DISTINCT r.entity_id), 2) AS per_entity\nFROM " + t
                                + " r\nLEFT JOIN " + c + " c ON c.entity_id = r.entity_id AND c.entity_version = r.entity_version\n"
                                + "GROUP BY r.sm_state\nORDER BY entities DESC"));
            }
        }
        trading(schema, out);
        out.add(new Sample("Sink offsets (Kafka position stored with the data)",
                "SELECT * FROM " + DuckDbTarget.OFFSETS_TABLE + " ORDER BY topic, \"partition\""));
        return out;
    }

    /**
     * Trading showcase queries, when the trading roots are configured (no market data here: ticks and
     * bars live in ClickHouse and Deephaven, so these stay within the concert snapshots).
     */
    static void trading(SchemaSpec schema, List<Sample> out) {
        var orders = schema.rootFor("trading_order").map(TableSpec::name).map(DuckDbTarget::q);
        var fills = schema.rootFor("trading_fill").map(TableSpec::name).map(DuckDbTarget::q);
        var allocations = schema.rootFor("trading_allocation").map(TableSpec::name).map(DuckDbTarget::q);
        fills.ifPresent(f -> out.add(new Sample("Trading: fills by venue and liquidity",
                "SELECT venue, liquidity, count(*) AS fills, sum(quantity) AS shares,\n"
                        + "       round(sum(price * quantity), 2) AS notional, round(sum(fee), 2) AS fees\nFROM " + f
                        + "\nGROUP BY ALL\nORDER BY notional DESC")));
        orders.ifPresent(o -> out.add(new Sample("Trading: implementation shortfall per order (avg px vs arrival, bps)",
                "SELECT order_id, symbol, side, order_type, sm_state, quantity, filled_qty, arrival_px, avg_px,\n"
                        + "       round((avg_px - arrival_px) / arrival_px * 10000 * CASE WHEN side = 'BUY' THEN 1 ELSE -1 END, 3)"
                        + " AS shortfall_bps\nFROM " + o + "\nWHERE filled_qty > 0\nORDER BY completed_at DESC\nLIMIT 100")));
        if (orders.isPresent() && fills.isPresent()) {
            out.add(new Sample("Trading: orders with their fills (joined on order_id)",
                    "SELECT o.order_id, o.symbol, o.side, o.sm_state, o.avg_px, count(f.fill_id) AS fills,\n"
                            + "       round(sum(f.price * f.quantity) / sum(f.quantity), 6) AS fill_vwap\nFROM " + orders.get() + " o\n"
                            + "LEFT JOIN " + fills.get() + " f ON f.order_id = o.order_id\nGROUP BY ALL\n"
                            + "ORDER BY fills DESC\nLIMIT 100"));
        }
        allocations.ifPresent(a -> out.add(new Sample("Trading: positions per account and symbol (allocations)",
                "SELECT account_id, symbol,\n"
                        + "       sum(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_qty,\n"
                        + "       round(sum(CASE WHEN side = 'BUY' THEN 1 ELSE -1 END * quantity * avg_px), 2) AS net_cost\n"
                        + "FROM " + a + "\nGROUP BY ALL\nORDER BY abs(net_cost) DESC\nLIMIT 100")));
    }

    /** Value-class groups with a DECIMAL {@code amount} and a {@code currency}, e.g. {@code total} of type Money. */
    static Map<String, ColumnSpec[]> moneyGroups(TableSpec table) {
        Map<String, ColumnSpec[]> groups = new LinkedHashMap<>();
        for (ColumnSpec c : table.modelColumns()) {
            if (c.group() == null) {
                continue;
            }
            if (c.leaf().equals("amount") && c.type() == ColumnType.DECIMAL) {
                groups.computeIfAbsent(c.group(), g -> new ColumnSpec[2])[0] = c;
            } else if (c.leaf().equals("currency") && (c.type() == ColumnType.ENUM || c.type() == ColumnType.STRING)) {
                groups.computeIfAbsent(c.group(), g -> new ColumnSpec[2])[1] = c;
            }
        }
        groups.values().removeIf(g -> g[0] == null || g[1] == null);
        return groups;
    }

    private static String cap(String s) {
        return s.isEmpty() ? s : Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }
}
