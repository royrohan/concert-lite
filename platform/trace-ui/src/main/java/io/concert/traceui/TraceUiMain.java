package io.concert.traceui;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.concert.common.Env;
import io.concert.common.Json;
import io.concert.common.TemporalClients;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EntitySnapshot;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.KeyLockWorkflow;
import io.concert.sdk.MachineCatalog;
import io.concert.sdk.StateMachineRegistry;
import io.concert.sdk.StateMachineSpec;
import io.concert.store.StateStore;
import io.concert.store.Stores;
import io.temporal.client.WorkflowClient;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event tracing UI at http://localhost:8088 (TRACE_UI_PORT). Reads DSQL {@code event_trace} /
 * {@code processed_event} / {@code sm_state} and queries live workflows in Temporal.
 *
 * <pre>
 * GET /api/recent                 latest finished / dropped events
 * GET /api/events/{eventId}       timeline of one event
 * GET /api/entities/{smType:key}  live state, history, Mermaid state diagram and model class diagram
 * GET /api/locks/{lockKey}        live holder and queue of a lock
 * /api/analytics/duckdb/{query,tables,samples,health}       proxied to the DuckDB sink (SINK_DUCKDB_URL)
 * /api/analytics/clickhouse/{query,tables,samples,health}   ClickHouse HTTP as the read-only user (CLICKHOUSE_URL,
 *                                                           CLICKHOUSE_USER / CLICKHOUSE_PASSWORD), same shapes
 * GET /api/analytics/deephaven/config   iframe base URL, PSK, widget and notebook names (DEEPHAVEN_*)
 * GET /api/analytics/lag                consumer-group lag per analytics target (KAFKA_BOOTSTRAP), console links,
 *                                       dead-letter counts per target, applied/sec per sink, ClickHouse kafka_errors
 * GET  /api/analytics/reconcile         the last on-demand reconciliation result
 * POST /api/analytics/reconcile?smType=&dryRun=   runs ReconcileWorkflow per typed smType (in its worker), waits
 * </pre>
 */
public final class TraceUiMain {
    private static final Logger log = LoggerFactory.getLogger(TraceUiMain.class);

    private final StateStore store;
    private final WorkflowClient client;
    private final String temporalUi = Env.get("TEMPORAL_UI_URL", "http://localhost:8080");
    /** The DuckDB sink's HTTP API; http://localhost:8090 when the trace UI runs outside compose. */
    private final String sinkDuckDb = Env.get("SINK_DUCKDB_URL", "http://sink-duckdb:8090");
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final ClickHouseConsole clickHouse = new ClickHouseConsole(Env.get("CLICKHOUSE_URL", "http://clickhouse:8123"),
            Env.get("CLICKHOUSE_USER", "readonly"), Env.get("CLICKHOUSE_PASSWORD", "readonly"), http);
    private final PipelineStatus pipeline = new PipelineStatus(Env.get("KAFKA_BOOTSTRAP", ""), http);
    /** Browser-facing URLs (the page runs on the host, not in the compose network). */
    private final String clickHousePublic = Env.get("CLICKHOUSE_PUBLIC_URL", "http://localhost:8123");
    private final String deephavenInternal = Env.get("DEEPHAVEN_URL", "http://deephaven:10000");
    private final String deephavenPublic = Env.get("DEEPHAVEN_PUBLIC_URL", "http://localhost:10000");
    /** Local dev only: the PSK goes to the page so the iframes can log in. */
    private final String deephavenPsk = Env.get("DEEPHAVEN_PSK", "concert");
    private final String consoleInternal = Env.get("REDPANDA_CONSOLE_URL", "http://redpanda-console:8080");
    private final String consolePublic = Env.get("REDPANDA_CONSOLE_PUBLIC_URL", "http://localhost:8081");
    private final ReconcileRunner reconcile;

    static final List<String> DEEPHAVEN_WIDGETS = List.of("quotes_latest", "vwap_by_symbol", "completions_per_minute",
            "state_distribution", "completion_latency", "revenue_by_currency", "quotes_with_sector", "orders_latest",
            "account_pnl", "pnl_by_account", "fill_slippage", "slippage_by_venue", "vwap_vs_arrival", "sector_exposure");
    static final List<String> DEEPHAVEN_NOTEBOOKS = List.of("01_live_quotes_and_spreads.py", "02_vwap_and_bars.py",
            "03_entity_completions.py", "04_slippage_asof.py", "05_trading_pnl.py");

    /**
     * Every state machine on the class path, by smType: each module that provides machines implements
     * the {@link MachineCatalog} SPI (sample workers, trading, and every generated ecosystem, which the
     * trace UI build adds as runtime dependencies automatically), so no module is named here.
     */
    static final Map<String, MachineCatalog.Machine> MACHINES = MachineCatalog.all();

    /** Mermaid class diagram of each typed machine's Pure model, by smType. */
    static final Map<String, String> MODEL_DIAGRAMS = modelDiagrams();

    private static Map<String, String> modelDiagrams() {
        Map<String, String> m = new LinkedHashMap<>();
        MACHINES.forEach((type, machine) -> {
            if (machine.modelMermaid() != null) {
                m.put(type, machine.modelMermaid());
            }
        });
        return Map.copyOf(m);
    }

    /** Registers the state machine specs the entity pages draw (every catalog's machines). */
    static void registerSpecs() {
        MACHINES.forEach((type, machine) -> StateMachineRegistry.register(type, machine.spec()));
    }

    TraceUiMain(StateStore store, WorkflowClient client) {
        this.store = store;
        this.client = client;
        this.reconcile = new ReconcileRunner(client, MACHINES);
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("STORE_INIT_SCHEMA", Env.get("STORE_INIT_SCHEMA", "false"));
        registerSpecs();
        log.info("state machines: {}", MACHINES.keySet());
        TraceUiMain ui = new TraceUiMain(Stores.fromEnv(), TemporalClients.fromEnv());
        int port = Env.getInt("TRACE_UI_PORT", 8088);
        ui.start(port);
        log.info("trace UI on http://localhost:{}", port);
    }

    HttpServer start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/api/recent", ex -> json(ex, Map.of("events", store.recentEvents(50))));
        server.createContext("/api/events/", ex -> json(ex, event(tail(ex, "/api/events/"))));
        server.createContext("/api/entities/", ex -> json(ex, entity(tail(ex, "/api/entities/"))));
        server.createContext("/api/locks/", ex -> json(ex, lock(tail(ex, "/api/locks/"))));
        server.createContext("/api/analytics/duckdb/", ex -> proxy(ex, sinkDuckDb, tail(ex, "/api/analytics/duckdb/")));
        server.createContext("/api/analytics/clickhouse/", ex -> clickHouse(ex, tail(ex, "/api/analytics/clickhouse/")));
        server.createContext("/api/analytics/deephaven/config", ex -> json(ex, deephavenConfig()));
        server.createContext("/api/analytics/lag", ex -> json(ex, lag()));
        server.createContext("/api/analytics/reconcile", this::reconcile);
        server.createContext("/", this::staticPage);
        server.start();
        return server;
    }

    Map<String, Object> event(String eventId) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("eventId", eventId);
        out.put("dedupeStatus", store.processedStatus(eventId).orElse(null));
        out.put("trace", store.traceForEvent(eventId));
        out.put("temporalUi", temporalUi + "/namespaces/default/workflows?query="
                + URLEncoder.encode("EventId=\"" + eventId + "\"", StandardCharsets.UTF_8));
        return out;
    }

    Map<String, Object> entity(String workflowId) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("workflowId", workflowId);
        String smType = workflowId.contains(":") ? workflowId.substring(0, workflowId.indexOf(':')) : workflowId;
        String state = null;
        try {
            EntitySnapshot snap = client.newWorkflowStub(EntityWorkflow.class, workflowId).snapshot();
            out.put("live", snap);
            state = snap.state();
        } catch (RuntimeException notRunning) {
            out.put("live", null);
        }
        store.loadState(workflowId).ifPresent(row -> out.put("projection", row));
        if (state == null && out.get("projection") instanceof io.concert.common.api.SmStateRow row) {
            state = row.state();
        }
        StateMachineSpec spec = StateMachineRegistry.get(smType);
        out.put("mermaid", spec == null ? null : spec.toMermaid(state));
        out.put("modelMermaid", MODEL_DIAGRAMS.get(smType));
        out.put("smType", smType);
        out.put("terminal", spec != null && state != null && spec.isTerminal(state));
        out.put("history", store.traceForWorkflow(workflowId, 100));
        out.put("temporalUi", temporalUi + "/namespaces/default/workflows/" + workflowId);
        return out;
    }

    Map<String, Object> lock(String lockKey) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("lockKey", lockKey);
        try {
            out.put("live", client.newWorkflowStub(KeyLockWorkflow.class, WorkflowIds.lock(lockKey)).snapshot());
        } catch (RuntimeException idle) {
            out.put("live", null); // no open lock workflow: the key is free
        }
        out.put("temporalUi", temporalUi + "/namespaces/default/workflows/" + WorkflowIds.lock(lockKey));
        return out;
    }

    private void clickHouse(HttpExchange ex, String endpoint) throws IOException {
        try {
            Map<String, Object> body = switch (endpoint) {
                case "query" -> {
                    if (!ex.getRequestMethod().equals("POST")) {
                        throw new ClickHouseConsole.QueryException(405, "POST the SQL statement as the request body");
                    }
                    String sql = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                    if (sql.stripLeading().startsWith("{")) {
                        sql = Json.MAPPER.readTree(sql).path("sql").asText(null);
                    }
                    yield clickHouse.query(sql);
                }
                case "tables" -> clickHouse.tables();
                case "samples" -> clickHouse.samples();
                case "health" -> clickHouse.health();
                default -> throw new ClickHouseConsole.QueryException(404, "unknown analytics endpoint " + endpoint);
            };
            json(ex, body);
        } catch (ClickHouseConsole.QueryException e) {
            json(ex, e.status, Map.of("error", e.getMessage()));
        }
    }

    Map<String, Object> deephavenConfig() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("url", deephavenPublic);
        out.put("psk", deephavenPsk);
        out.put("reachable", pipeline.reachable(deephavenInternal + "/ide/"));
        out.put("iframe", deephavenPublic + "/iframe/widget/?name=");
        out.put("ide", deephavenPublic + "/ide/?psk=" + URLEncoder.encode(deephavenPsk, StandardCharsets.UTF_8));
        out.put("widgets", DEEPHAVEN_WIDGETS);
        out.put("notebooks", DEEPHAVEN_NOTEBOOKS);
        return out;
    }

    Map<String, Object> lag() {
        Map<String, Object> out = new LinkedHashMap<>(pipeline.lag());
        out.put("console", Map.of("url", consolePublic, "reachable", pipeline.reachable(consoleInternal)));
        out.put("clickhousePlay", clickHousePublic + "/play");
        out.put("dlq", pipeline.deadLetters());
        out.put("sinks", sinkRates());
        return out;
    }

    /**
     * Applied/sec per sink (from counter deltas between calls): DuckDB's applied entities from its
     * {@code /health}; ClickHouse's messages read by its Kafka engine queues plus its {@code kafka_errors} count.
     */
    Map<String, Object> sinkRates() {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(sinkDuckDb + "/health"))
                    .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString());
            var h = Json.MAPPER.readTree(r.body());
            long applied = h.path("target").path("appliedEntities").asLong();
            Map<String, Object> d = new LinkedHashMap<>();
            d.put("applied", applied);
            d.put("appliedPerSec", pipeline.rate("duckdb", applied));
            d.put("dlq", h.path("dlq").asLong());
            d.put("lag", h.path("consumer").path("lag").asLong());
            out.put("duckdb", d);
        } catch (IOException | RuntimeException e) {
            out.put("duckdb", Map.of("error", "not reachable"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            Map<String, Object> h = clickHouse.health();
            long read = 0;
            if (h.get("kafkaConsumers") instanceof List<?> l) {
                for (Object o : l) {
                    if (o instanceof Map<?, ?> m && String.valueOf(m.get("table")).startsWith("entity_snapshots")) {
                        read += ((Number) m.get("messagesRead")).longValue();
                    }
                }
            }
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("read", read);
            c.put("readPerSec", pipeline.rate("clickhouse", read));
            c.put("kafkaErrors", h.get("parseErrors"));
            out.put("clickhouse", c);
        } catch (ClickHouseConsole.QueryException | RuntimeException e) {
            out.put("clickhouse", Map.of("error", "not reachable"));
        }
        return out;
    }

    private void reconcile(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) {
            json(ex, reconcile.last());
            return;
        }
        Map<String, String> q = new LinkedHashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw != null) {
            for (String kv : raw.split("&")) {
                int i = kv.indexOf('=');
                q.put(URLDecoder.decode(i < 0 ? kv : kv.substring(0, i), StandardCharsets.UTF_8),
                        i < 0 ? "true" : URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
            }
        }
        json(ex, reconcile.run(q.get("smType"), Boolean.parseBoolean(q.getOrDefault("dryRun", "false"))));
    }

    /**
     * Forwards an analytics call to a sink's HTTP API (only its known endpoints), passing method, body
     * and status through; an unreachable sink gives 502 with an explanation for the page.
     */
    private void proxy(HttpExchange ex, String base, String endpoint) throws IOException {
        if (!List.of("query", "tables", "samples", "health").contains(endpoint)) {
            json(ex, 404, Map.of("error", "unknown analytics endpoint " + endpoint));
            return;
        }
        byte[] body = ex.getRequestBody().readAllBytes();
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(base + "/" + endpoint)).timeout(Duration.ofSeconds(30));
        req = ex.getRequestMethod().equals("POST")
                ? req.POST(HttpRequest.BodyPublishers.ofByteArray(body)).header("Content-Type", "text/plain; charset=utf-8")
                : req.GET();
        HttpResponse<byte[]> resp;
        try {
            resp = http.send(req.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            json(ex, 502, Map.of("error", "sink not reachable at " + base + " (" + e + "); start it with "
                    + "scripts/up.sh <store> --analytics"));
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            json(ex, 502, Map.of("error", "interrupted"));
            return;
        }
        ex.getResponseHeaders().add("Content-Type", resp.headers().firstValue("Content-Type").orElse("application/json"));
        ex.sendResponseHeaders(resp.statusCode(), resp.body().length);
        ex.getResponseBody().write(resp.body());
        ex.close();
    }

    private static String tail(HttpExchange ex, String prefix) {
        return URLDecoder.decode(ex.getRequestURI().getRawPath().substring(prefix.length()), StandardCharsets.UTF_8);
    }

    private static void json(HttpExchange ex, Object body) throws IOException {
        json(ex, 200, body);
    }

    private static void json(HttpExchange ex, int okStatus, Object body) throws IOException {
        byte[] bytes;
        int status = okStatus;
        try {
            bytes = Json.write(body).getBytes(StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            status = 500;
            bytes = Json.write(Map.of("error", String.valueOf(e.getMessage()))).getBytes(StandardCharsets.UTF_8);
        }
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }

    private void staticPage(HttpExchange ex) throws IOException {
        try (InputStream in = TraceUiMain.class.getResourceAsStream("/web/index.html")) {
            byte[] bytes = in.readAllBytes();
            ex.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(200, bytes.length);
            ex.getResponseBody().write(bytes);
        }
        ex.close();
    }
}
