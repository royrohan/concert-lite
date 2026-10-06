package io.concert.sink.clickhouse;

import io.concert.sink.ColumnSpec;
import io.concert.sink.ColumnType;
import io.concert.sink.SchemaSpec;
import io.concert.sink.TableSpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Example queries for the trace UI's ClickHouse console, generated from the schema (like sink-duckdb's)
 * and stored by the provisioner in {@value ClickHouseSchema#SAMPLES}, where the trace UI reads them
 * as the read-only user.
 */
public final class ClickHouseSamples {

    public record Sample(String title, String sql) {}

    private ClickHouseSamples() {}

    public static List<Sample> generate(SchemaSpec schema, boolean marketData) {
        List<Sample> out = new ArrayList<>();
        for (TableSpec root : schema.roots()) {
            String t = Sql.id(root.name());
            String preview = root.modelColumns().stream().filter(c -> c.type() != ColumnType.JSON).limit(6)
                    .map(c -> Sql.id(c.name())).collect(Collectors.joining(", "));
            out.add(new Sample("Completed " + root.smType() + " entities (FINAL)",
                    "SELECT entity_id, sm_state, entity_version, created_at, completed_at"
                            + (preview.isEmpty() ? "" : ", " + preview) + "\nFROM " + t + " FINAL\nORDER BY completed_at DESC\nLIMIT 100"));
            out.add(new Sample(cap(root.smType()) + " count and time to complete by state",
                    "SELECT sm_state, count() AS n,\n       round(avg(dateDiff('millisecond', created_at, completed_at)) / 1000, 3) AS avg_seconds\n"
                            + "FROM " + t + " FINAL\nGROUP BY sm_state\nORDER BY n DESC"));
            for (Map.Entry<String, ColumnSpec[]> money : moneyGroups(root).entrySet()) {
                String amount = Sql.id(money.getValue()[0].name());
                String currency = Sql.id(money.getValue()[1].name());
                out.add(new Sample(cap(root.smType()) + " " + money.getKey() + " by currency",
                        "SELECT " + currency + " AS currency, sm_state, count() AS n, sum(" + amount + ") AS total, avg(" + amount
                                + ") AS average\nFROM " + t + " FINAL\nWHERE " + amount + " IS NOT NULL\nGROUP BY currency, sm_state\nORDER BY total DESC"));
                break;
            }
            for (TableSpec child : schema.childrenOf(root)) {
                String arr = Sql.id(ClickHouseSchema.arrayColumn(root, child));
                out.add(new Sample(cap(root.smType()) + " " + child.sourcePath().getLast() + " (ARRAY JOIN view " + child.name() + ")",
                        "SELECT r.sm_state, c.*\nFROM " + Sql.id(child.name()) + " AS c\nINNER JOIN " + t
                                + " AS r FINAL ON r.entity_id = c.entity_id\nORDER BY r.completed_at DESC, c.idx\nLIMIT 200"));
                out.add(new Sample(cap(child.sourcePath().getLast()) + " per " + root.smType() + " (array column)",
                        "SELECT entity_id, sm_state, length(" + arr + ") AS n, " + arr + "\nFROM " + t
                                + " FINAL\nORDER BY completed_at DESC\nLIMIT 50"));
            }
            out.add(new Sample(cap(root.smType()) + " rows stored vs entities (ReplacingMergeTree)",
                    "SELECT (SELECT count() FROM " + t + ") AS rows_stored, (SELECT count() FROM " + t + " FINAL) AS entities,\n"
                            + "       (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '" + root.name()
                            + "' AND active) AS active_parts"));
        }
        if (marketData) {
            out.add(new Sample("Latest quote per symbol",
                    "SELECT symbol, argMax(bid, ts) AS bid, argMax(ask, ts) AS ask, argMax(mid, ts) AS mid,\n"
                            + "       argMax(last, ts) AS last, max(ts) AS ts, count() AS ticks\nFROM ticks\nGROUP BY symbol\nORDER BY symbol"));
            out.add(new Sample("Ticks per second (last 5 min)",
                    "SELECT toStartOfSecond(ts) AS second, count() AS ticks, uniqExact(symbol) AS symbols\nFROM ticks\n"
                            + "WHERE ts > now64(3) - INTERVAL 5 MINUTE\nGROUP BY second\nORDER BY second DESC\nLIMIT 60"));
            out.add(new Sample("1-minute bars (md.bars.1m)",
                    "SELECT symbol, start, open, high, low, close, volume, vwap, trades\nFROM bars_1m\nORDER BY start DESC, symbol\nLIMIT 100"));
            out.add(new Sample("1-minute bars computed from ticks",
                    "SELECT symbol, toStartOfMinute(ts) AS minute, argMin(last, ts) AS open, max(last) AS high, min(last) AS low,\n"
                            + "       argMax(last, ts) AS close, sum(last_size) AS volume,\n"
                            + "       round(sum(last * last_size) / nullIf(sum(last_size), 0), 4) AS vwap\n"
                            + "FROM ticks\nWHERE last_size > 0\nGROUP BY symbol, minute\nORDER BY minute DESC, symbol\nLIMIT 100"));
            out.add(new Sample("Quotes joined with instruments, by sector",
                    "WITH q AS (SELECT symbol, argMax(mid, ts) AS mid FROM ticks GROUP BY symbol)\n"
                            + "SELECT i.sector, count() AS symbols,\n"
                            + "       round(avg(toFloat64((q.mid - i.ref_price) / i.ref_price)) * 100, 3) AS avg_move_vs_ref_pct\n"
                            + "FROM q\nINNER JOIN instruments AS i FINAL ON i.symbol = q.symbol\nGROUP BY i.sector\nORDER BY avg_move_vs_ref_pct DESC"));
            out.add(new Sample("VWAP and traded volume by symbol (last 15 min)",
                    "SELECT t.symbol, i.name, i.sector, sum(t.last_size) AS volume,\n"
                            + "       round(sum(t.last * t.last_size) / nullIf(sum(t.last_size), 0), 4) AS vwap\n"
                            + "FROM ticks AS t\nLEFT JOIN instruments AS i FINAL ON i.symbol = t.symbol\n"
                            + "WHERE t.ts > now64(3) - INTERVAL 15 MINUTE AND t.last_size > 0\n"
                            + "GROUP BY t.symbol, i.name, i.sector\nORDER BY volume DESC\nLIMIT 50"));
            out.add(new Sample("Reference data: accounts and venues",
                    "SELECT 'account' AS kind, account_id AS id, name, type FROM accounts FINAL\nUNION ALL\n"
                            + "SELECT 'venue', mic, name, type FROM venues FINAL\nORDER BY kind, id"));
        }
        out.add(new Sample("Kafka consumers (ClickHouse Kafka engine)",
                "SELECT table, num_messages_read, last_poll_time, last_commit_time, is_currently_used,\n"
                        + "       arrayStringConcat(exceptions.text, ' | ') AS errors\nFROM system.kafka_consumers\n"
                        + "WHERE database = currentDatabase()\nORDER BY table"));
        out.add(new Sample("Unparsable Kafka records",
                "SELECT at, source, topic, kafka_partition, kafka_offset, error, raw_message\nFROM " + ClickHouseSchema.ERRORS
                        + "\nORDER BY at DESC\nLIMIT 100"));
        return out;
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
