package io.concert.sink.duckdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.sink.EntitySnapshot;
import io.concert.sink.SchemaMapper;
import io.concert.sink.SchemaSpec;
import io.concert.sink.SnapshotRecord;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DuckDbTargetTest {

    static final Path MODELS = Path.of(System.getProperty("concert.modelsDir", "../../showcases/sample-workers/src/main/pure"));
    static final String T = "entity-snapshots";

    static SchemaSpec schema() {
        return new SchemaMapper(SchemaMapper.loadModels(MODELS)).map(SchemaMapper.parseRoots(SchemaMapper.DEFAULT_ROOTS));
    }

    /** An order snapshot with the given lines, each {@code sku:qty:price} in EUR. */
    static EntitySnapshot order(String id, long version, String... lines) {
        StringBuilder ls = new StringBuilder();
        double total = 0;
        for (String l : lines) {
            String[] p = l.split(":");
            ls.append(ls.isEmpty() ? "" : ",").append("{\"quantity\":").append(p[1]).append(",\"sku\":\"").append(p[0])
                    .append("\",\"unitPrice\":{\"amount\":").append(p[2]).append(",\"currency\":\"EUR\"}}");
            total += Integer.parseInt(p[1]) * Double.parseDouble(p[2]);
        }
        String json = "{\"customer\":{\"customerId\":\"c1\",\"name\":\"Ann\"},\"deliveredAt\":\"2026-10-05T10:00:03Z\",\"lines\":["
                + ls + "],\"orderId\":\"" + id + "\",\"paymentMethod\":\"CARD\",\"status\":\"DELIVERED\",\"total\":{\"amount\":"
                + String.format("%.2f", total) + ",\"currency\":\"EUR\"}}";
        return new EntitySnapshot("order:" + id, "order", "DELIVERED", version, Instant.parse("2026-10-05T10:00:00Z"),
                Instant.parse("2026-10-05T10:00:03Z"), json);
    }

    @TempDir
    Path dir;
    DuckDbTarget db;
    ReadOnlyQueries queries;

    @BeforeEach
    void open() {
        db = new DuckDbTarget(dir.resolve("test.duckdb").toString());
        db.ensureSchema(schema());
        queries = new ReadOnlyQueries(db, Duration.ofSeconds(10));
    }

    @AfterEach
    void close() {
        queries.close();
        db.close();
    }

    List<List<Object>> rows(String sql) throws SQLException {
        return queries.run(sql).rows();
    }

    static SnapshotRecord rec(EntitySnapshot s, int partition, long offset) {
        return new SnapshotRecord(s, T, partition, offset);
    }

    @Test
    void upsertIsIdempotent() throws SQLException {
        List<SnapshotRecord> batch = List.of(rec(order("o1", 3, "A:2:10.50", "B:1:4"), 0, 0), rec(order("o2", 3, "C:1:1"), 1, 0));
        db.apply(batch);
        List<List<Object>> first = rows("SELECT * FROM orders ORDER BY entity_id");
        db.apply(batch);
        assertEquals(first, rows("SELECT * FROM orders ORDER BY entity_id"));
        assertEquals(2, first.size());
        assertEquals(List.of(List.of("order:o1", 3L, "A", 2L, "10.5000", "EUR")),
                rows("SELECT entity_id, entity_version, sku, quantity, unit_price_amount::VARCHAR, unit_price_currency"
                        + " FROM order_lines WHERE idx = 0 AND entity_id = 'order:o1'"));
        assertEquals(List.of(List.of(3L)), rows("SELECT count(*) FROM order_lines"));
        assertEquals(List.of(List.of("25.0000", "EUR", "Ann", "2026-10-05 10:00:03+00")),
                rows("SELECT total_amount::VARCHAR, total_currency, customer_name, delivered_at::VARCHAR FROM orders WHERE entity_id = 'order:o1'"));
        assertEquals(List.of(List.of("o1", "CARD")), rows("SELECT model_json->>'orderId', model_json->>'paymentMethod'"
                + " FROM orders WHERE entity_id = 'order:o1'"));
        assertEquals(2L, db.stats().appliedEntities());
        assertEquals(2L, db.stats().staleSkipped());
    }

    @Test
    void olderVersionIsIgnored() throws SQLException {
        db.apply(List.of(rec(order("o1", 5, "A:1:1"), 0, 0)));
        db.apply(List.of(rec(order("o1", 4, "X:9:9", "Y:1:1"), 0, 1)));
        assertEquals(List.of(List.of(5L, "1.0000")), rows("SELECT entity_version, total_amount::VARCHAR FROM orders"));
        assertEquals(List.of(List.of("A")), rows("SELECT sku FROM order_lines"));
        // within one batch only the highest version counts, whatever the order
        db.apply(List.of(rec(order("o2", 7, "N:1:1"), 0, 2), rec(order("o2", 6, "O:1:1"), 0, 3)));
        assertEquals(List.of(List.of(7L, "N")),
                rows("SELECT o.entity_version, l.sku FROM orders o JOIN order_lines l USING (entity_id) WHERE entity_id = 'order:o2'"));
        assertEquals(Map.of(new TopicPartition(T, 0), 4L), db.storedOffsets());
    }

    @Test
    void childRowsAreReplacedWhenTheVersionAdvances() throws SQLException {
        db.apply(List.of(rec(order("o1", 3, "A:1:1", "B:1:1", "C:1:1"), 0, 0)));
        db.apply(List.of(rec(order("o1", 4, "D:2:5"), 0, 1)));
        assertEquals(List.of(List.of(0L, "D", 4L)), rows("SELECT idx, sku, entity_version FROM order_lines"));
        db.apply(List.of(rec(order("o1", 5), 0, 2)));
        assertEquals(List.of(List.of(0L)), rows("SELECT count(*) FROM order_lines"));
    }

    @Test
    void offsetsAreStoredInTheSameTransaction() throws SQLException {
        db.apply(List.of(rec(order("o1", 3, "A:1:1"), 0, 41), rec(order("o2", 3, "A:1:1"), 2, 7)));
        assertEquals(Map.of(new TopicPartition(T, 0), 42L, new TopicPartition(T, 2), 8L), db.storedOffsets());

        db.beforeCommit = () -> {
            throw new IllegalStateException("crash before commit");
        };
        assertThrows(IllegalStateException.class, () -> db.apply(List.of(
                rec(order("o3", 3, "A:1:1"), 0, 42), rec(order("o1", 9, "Z:1:1"), 2, 8))));
        // nothing of the failed batch is visible: rows, children and offsets are unchanged
        assertEquals(List.of(List.of("order:o1", 3L), List.of("order:o2", 3L)),
                rows("SELECT entity_id, entity_version FROM orders ORDER BY 1"));
        assertEquals(List.of(List.of("A"), List.of("A")), rows("SELECT sku FROM order_lines"));
        assertEquals(Map.of(new TopicPartition(T, 0), 42L, new TopicPartition(T, 2), 8L), db.storedOffsets());

        db.beforeCommit = () -> {};
        db.apply(List.of(rec(order("o3", 3, "A:1:1"), 0, 42)));
        assertEquals(43L, db.storedOffsets().get(new TopicPartition(T, 0)));
    }

    @Test
    void unknownSmTypesAreSkippedAndCounted() throws SQLException {
        db.apply(List.of(rec(new EntitySnapshot("ledger:1", "ledger", "CLOSED", 2, null, Instant.EPOCH, "{\"x\":1}"), 0, 0)));
        assertEquals(Map.of("ledger", 1L), db.stats().unknownSmTypes());
        assertEquals(1L, db.storedOffsets().get(new TopicPartition(T, 0)));
        assertEquals(List.of(List.of(0L)), rows("SELECT count(*) FROM orders"));
    }

    @Test
    void schemaIsCreatedOnceAndReopens() throws SQLException {
        db.apply(List.of(rec(order("o1", 3, "A:1:1"), 0, 0)));
        db.close();
        queries.close();
        db = new DuckDbTarget(dir.resolve("test.duckdb").toString());
        db.ensureSchema(schema()); // idempotent
        queries = new ReadOnlyQueries(db, Duration.ofSeconds(10));
        assertEquals(List.of(List.of(1L)), rows("SELECT count(*) FROM orders"));
        assertEquals(1L, db.storedOffsets().get(new TopicPartition(T, 0)));
    }

    @Test
    void queryEndpointRejectsWrites() throws SQLException {
        db.apply(List.of(rec(order("o1", 3, "A:1:1"), 0, 0)));
        for (String bad : List.of("DELETE FROM orders", "drop table orders", "INSERT INTO orders (entity_id) VALUES ('x')",
                "UPDATE orders SET sm_state = 'X'", "SELECT 1; DELETE FROM orders", "COPY orders TO '/tmp/x.csv'",
                "ATTACH '/tmp/other.db'", "SET enable_external_access = true", "PRAGMA enable_profiling",
                "EXPLAIN ANALYZE DELETE FROM orders", "CHECKPOINT", "  -- comment\n DELETE FROM orders", "")) {
            assertThrows(IllegalArgumentException.class, () -> queries.run(bad), bad);
        }
        // reading the host's files is disabled for the whole database
        assertThrows(SQLException.class, () -> queries.run("SELECT * FROM read_csv('/etc/hosts')"));
        assertEquals(List.of(List.of(1L)), rows("SELECT count(*) FROM orders;"));
        assertEquals(List.of(List.of("a;b")), rows("SELECT 'a;b' -- trailing ; comment"));
        assertTrue(queries.run("DESCRIBE orders").rows().size() > 10);
        assertTrue(queries.run("PRAGMA table_info('order_lines')").rows().size() >= 7);
        assertTrue(queries.run("SUMMARIZE orders").rows().size() > 10);
        ReadOnlyQueries.Result many = queries.run("SELECT * FROM range(6000)");
        assertEquals(ReadOnlyQueries.MAX_ROWS, many.rows().size());
        assertTrue(many.truncated());
    }

    @Test
    void sampleQueriesRun() throws SQLException {
        db.apply(List.of(rec(order("o1", 3, "A:1:1"), 0, 0)));
        List<SampleQueries.Sample> samples = SampleQueries.generate(db.schema());
        assertTrue(samples.stream().anyMatch(s -> s.title().equals("Order total by currency")), samples.toString());
        for (SampleQueries.Sample s : samples) {
            queries.run(s.sql()); // every sample is valid, read-only SQL for the schema
        }
    }

    static final Path TRADING = Path.of(System.getProperty("concert.tradingModelsDir", "../../core/trading-model/src/main/pure"));

    @Test
    void tradingRootsGetTheirOwnTablesAndSamplesRun() throws SQLException {
        DuckDbTarget t = new DuckDbTarget(dir.resolve("trading.duckdb").toString());
        try (t) {
            t.ensureSchema(new SchemaMapper(SchemaMapper.loadModels(MODELS + "," + TRADING)).map(SchemaMapper.parseRoots(
                    SchemaMapper.DEFAULT_ROOTS + ",trading_order:trading::order::Order,trading_execution:trading::order::Execution,"
                            + "trading_fill:trading::order::Fill,trading_allocation:trading::order::Allocation")));
            Instant at = Instant.parse("2026-10-05T14:30:00Z");
            t.apply(List.of(
                    rec(new EntitySnapshot("trading_order:O1", "trading_order", "FILLED", 9, at, at,
                            "{\"accountId\":\"ACC-1\",\"allocations\":[],\"arrivalPx\":100.00,\"avgPx\":100.01,\"executions\":[],"
                                    + "\"filledQty\":100,\"orderId\":\"O1\",\"orderType\":\"MARKET\",\"quantity\":100,\"side\":\"BUY\","
                                    + "\"status\":\"FILLED\",\"symbol\":\"AAPL\"}"), 0, 0),
                    rec(new EntitySnapshot("trading_fill:F1", "trading_fill", "BOOKED", 1, at, at,
                            "{\"accountId\":\"ACC-1\",\"execId\":\"E1\",\"fee\":0.05,\"fillId\":\"F1\",\"liquidity\":\"TAKER\","
                                    + "\"orderId\":\"O1\",\"price\":100.01,\"quantity\":100,\"side\":\"BUY\",\"status\":\"BOOKED\","
                                    + "\"symbol\":\"AAPL\",\"ts\":\"2026-10-05T14:30:00.500Z\"}"), 0, 1)));
            ReadOnlyQueries q = new ReadOnlyQueries(t, Duration.ofSeconds(10));
            try (q) {
                List<Object> names = q.run("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'trading%'"
                        + " ORDER BY table_name").rows().stream().map(r -> r.get(0)).toList();
                assertEquals(List.of("trading_allocations", "trading_executions", "trading_fills", "trading_orders"), names);
                assertEquals(List.of(List.of("O1", 1L)), q.run("SELECT o.order_id, count(*) FROM trading_orders o"
                        + " JOIN trading_fills f ON f.order_id = o.order_id GROUP BY ALL").rows());
                List<SampleQueries.Sample> samples = SampleQueries.generate(t.schema());
                assertEquals(4, samples.stream().filter(s -> s.title().startsWith("Trading:")).count());
                for (SampleQueries.Sample s : samples) {
                    q.run(s.sql());
                }
            }
        }
    }
}
