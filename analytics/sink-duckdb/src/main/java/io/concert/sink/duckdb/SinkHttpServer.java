package io.concert.sink.duckdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.concert.sink.SinkMetrics;
import io.concert.sink.SnapshotConsumerLoop;
import io.concert.sink.TableSpec;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * HTTP API of the DuckDB sink (proxied by the trace UI under {@code /api/analytics/duckdb/}):
 *
 * <pre>
 * POST /query    body = one read-only SQL statement (or {"sql": "..."}) -> {columns, types, rows, elapsedMs, truncated}
 * GET  /tables   tables and columns, with the smType of root tables
 * GET  /samples  example queries generated from the models
 * POST /versions {"smType": "order", "entityIds": [...]} -> {smType, table, versions: {entityId: entity_version}}
 *                (read-only; for reconciliation; table null when the smType is not a sink root; max 5000 ids)
 * GET  /health   consumer and apply counters (incl. dead-lettered records), stored offsets
 * GET  /metrics  the same counters, batch latency p50 / p99 and lag in Prometheus text format
 * </pre>
 *
 * There is no endpoint that writes: the consumer loop is the only writer of the database.
 */
public final class SinkHttpServer implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    static final int MAX_VERSION_IDS = 5000;

    private final DuckDbTarget db;
    private final ReadOnlyQueries queries;
    private final Supplier<SnapshotConsumerLoop.Stats> consumerStats;
    private final SinkMetrics metrics;
    private HttpServer server;

    /** @param consumerStats the consumer loop's counters, or {@code null} if none runs */
    public SinkHttpServer(DuckDbTarget db, ReadOnlyQueries queries, Supplier<SnapshotConsumerLoop.Stats> consumerStats) {
        this(db, queries, consumerStats, null);
    }

    /** With the loop's latency metrics. */
    public SinkHttpServer(DuckDbTarget db, ReadOnlyQueries queries, SnapshotConsumerLoop loop) {
        this(db, queries, loop::stats, loop.metrics());
    }

    private SinkHttpServer(DuckDbTarget db, ReadOnlyQueries queries, Supplier<SnapshotConsumerLoop.Stats> consumerStats,
            SinkMetrics metrics) {
        this.db = db;
        this.queries = queries;
        this.consumerStats = consumerStats;
        this.metrics = metrics;
    }

    public SinkHttpServer start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/query", this::query);
        server.createContext("/tables", ex -> respond(ex, 200, tables()));
        server.createContext("/samples", ex -> respond(ex, 200, Map.of("samples", SampleQueries.generate(db.schema()))));
        server.createContext("/health", ex -> respond(ex, 200, health()));
        server.createContext("/versions", this::versions);
        server.createContext("/metrics", this::metrics);
        server.start();
        return this;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private void query(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) {
            respond(ex, 405, Map.of("error", "POST the SQL statement as the request body"));
            return;
        }
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String sql = body;
        if (body.stripLeading().startsWith("{")) {
            JsonNode n = JSON.readTree(body);
            sql = n.path("sql").asText(null);
        }
        try {
            respond(ex, 200, queries.run(sql));
        } catch (IllegalArgumentException e) {
            respond(ex, 403, Map.of("error", e.getMessage()));
        } catch (SQLException e) {
            respond(ex, 400, Map.of("error", e.getMessage()));
        }
    }

    private void versions(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) {
            respond(ex, 405, Map.of("error", "POST {\"smType\": ..., \"entityIds\": [...]}"));
            return;
        }
        JsonNode req;
        try {
            req = JSON.readTree(ex.getRequestBody());
        } catch (IOException e) {
            respond(ex, 400, Map.of("error", "body is not JSON: " + e.getMessage()));
            return;
        }
        String smType = req == null ? null : req.path("smType").asText(null);
        JsonNode ids = req == null ? null : req.get("entityIds");
        if (smType == null || ids == null || !ids.isArray()) {
            respond(ex, 400, Map.of("error", "smType and entityIds are required"));
            return;
        }
        if (ids.size() > MAX_VERSION_IDS) {
            respond(ex, 400, Map.of("error", "at most " + MAX_VERSION_IDS + " entityIds per request"));
            return;
        }
        List<String> list = new ArrayList<>(ids.size());
        ids.forEach(n -> list.add(n.asText()));
        try {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("smType", smType);
            TableSpec root = db.schema() == null ? null : db.schema().rootFor(smType).orElse(null);
            out.put("table", root == null ? null : root.name());
            out.put("versions", root == null ? Map.of() : db.versions(root, list));
            respond(ex, 200, out);
        } catch (SQLException e) {
            respond(ex, 500, Map.of("error", e.getMessage()));
        }
    }

    private void metrics(HttpExchange ex) throws IOException {
        byte[] bytes = prometheus().getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        ex.sendResponseHeaders(200, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }

    /** Prometheus text exposition of the sink's counters. */
    String prometheus() {
        Map<String, String> t = Map.of("target", "duckdb");
        DuckDbTarget.Stats ts = db.stats();
        SinkMetrics.Text out = new SinkMetrics.Text()
                .metric("concert_sink_applied_total", "counter", "Entity snapshots written (version advanced).", t,
                        ts.appliedEntities())
                .metric("concert_sink_stale_total", "counter", "Snapshots skipped: an equal or newer version was stored.", t,
                        ts.staleSkipped())
                .metric("concert_sink_unknown_smtype_total", "counter", "Snapshots of smTypes that are not sink roots.", t,
                        ts.unknownSmTypes().values().stream().mapToLong(Long::longValue).sum());
        SnapshotConsumerLoop.Stats cs = consumerStats == null ? null : consumerStats.get();
        if (cs != null) {
            out.metric("concert_sink_records_total", "counter", "Records consumed from the snapshot topic.", t, cs.records())
                    .metric("concert_sink_batches_total", "counter", "Batches applied.", t, cs.batches())
                    .metric("concert_sink_undecodable_total", "counter", "Records that are not snapshots.", t, cs.undecodable())
                    .metric("concert_sink_dlq_total", "counter", "Records sent to the dead-letter topic.", t, cs.dlq())
                    .metric("concert_sink_apply_failures_total", "counter", "Failed apply attempts (retried).", t, cs.failures())
                    .metric("concert_sink_lag_records", "gauge", "Records behind the end of the assigned partitions.", t, cs.lag());
        }
        if (metrics != null) {
            out.header("concert_sink_batch_latency_seconds", "summary", "Batch apply latency (last 2048 batches).");
            out.sample("concert_sink_batch_latency_seconds", Map.of("target", "duckdb", "quantile", "0.5"), metrics.quantileSeconds(0.5));
            out.sample("concert_sink_batch_latency_seconds", Map.of("target", "duckdb", "quantile", "0.99"), metrics.quantileSeconds(0.99));
            out.sample("concert_sink_batch_latency_seconds_sum", t, metrics.batchSumSeconds());
            out.sample("concert_sink_batch_latency_seconds_count", t, metrics.batchCount());
        }
        return out.toString();
    }

    Map<String, Object> tables() {
        Map<String, Map<String, Object>> byName = new LinkedHashMap<>();
        try (Connection c = db.newReadConnection(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT table_name, column_name, data_type FROM information_schema.columns"
                        + " WHERE table_schema = 'main' ORDER BY table_name, ordinal_position")) {
            while (rs.next()) {
                String table = rs.getString(1);
                Map<String, Object> t = byName.computeIfAbsent(table, name -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("name", name);
                    TableSpec spec = db.schema() == null ? null : db.schema().table(name).orElse(null);
                    m.put("kind", spec == null ? "system" : spec.kind().name().toLowerCase(Locale.ROOT));
                    m.put("smType", spec == null ? null : spec.smType());
                    m.put("parent", spec == null ? null : spec.parent());
                    m.put("columns", new ArrayList<Map<String, String>>());
                    return m;
                });
                @SuppressWarnings("unchecked")
                List<Map<String, String>> cols = (List<Map<String, String>>) t.get("columns");
                cols.add(Map.of("name", rs.getString(2), "type", rs.getString(3)));
            }
        } catch (SQLException e) {
            return Map.of("error", e.getMessage());
        }
        return Map.of("tables", List.copyOf(byName.values()));
    }

    Map<String, Object> health() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", "ok");
        out.put("target", db.stats());
        SnapshotConsumerLoop.Stats cs = consumerStats == null ? null : consumerStats.get();
        out.put("consumer", cs);
        out.put("dlq", cs == null ? 0 : cs.dlq());
        if (metrics != null) {
            out.put("batchLatencyMs", Map.of("p50", metrics.quantileSeconds(0.5) * 1000, "p99", metrics.quantileSeconds(0.99) * 1000));
        }
        Map<String, Long> offsets = new LinkedHashMap<>();
        db.storedOffsets().entrySet().stream()
                .sorted(Map.Entry.comparingByKey((a, b) -> a.toString().compareTo(b.toString())))
                .forEach(e -> offsets.put(e.getKey().toString(), e.getValue()));
        out.put("storedOffsets", offsets);
        return out;
    }

    private static void respond(HttpExchange ex, int status, Object body) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(1);
        }
    }
}
