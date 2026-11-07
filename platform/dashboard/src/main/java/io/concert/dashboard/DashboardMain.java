package io.concert.dashboard;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.concert.common.Env;
import io.concert.common.Flows;
import io.concert.common.Json;
import io.concert.common.TemporalClients;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Concert Dashboard at http://localhost:8088 (DASHBOARD_PORT): flows, their operations and event tracing. Reads DSQL {@code event_trace} /
 * {@code processed_event} / {@code sm_state} and queries live workflows in Temporal.
 *
 * <pre>
 * GET /api/flows?window=60       flows by catalog (entity smTypes, event domains) with headline counts
 * GET /api/flows/{flow}?window=60  per-hop metrics of one flow: publisher, Kinesis, coordinator, worker, Kafka, stores
 * GET  /api/flows/{flow}/generate                          test-data generator status of the flow
 * POST /api/flows/{flow}/generate?rate=&duration=&errors=   start it (instances/s, seconds, inject failures)
 * POST /api/flows/{flow}/generate/stop                     stop it
 * GET /api/flows/{flow}/temporal?window=300  serial key queues, workflows by status, task queues, ingest consumers
 * GET  /api/perf           Performance page: presets, flows, the current run (settings, timeline, load), its instances,
 *                          Kafka and analytics service states
 * POST /api/perf/start?flow=&preset=&machine=&coordinators=&workers=&rate=&duration=&chaos=&chaosEvery=&chaosKafka=&stopAnalytics=
 *                          one flow at a time: stops the others (and analytics if asked), starts it scaled, loads it
 * POST /api/perf/stop · /api/perf/restore · /api/perf/chaos/{on|off} · /api/perf/kafka/{stop|start}
 *                          (start also takes lockMode=serial|entity|direct)
 * GET  /api/perf/runs?limit=        past runs (DuckDB, PERF_DB) with summaries, newest first
 * GET  /api/perf/compare?ids=a,b    up to 4 runs side by side: settings, summary, 10 s samples, timeline
 * POST /api/perf/runs/{runId}/delete
 * POST /api/perf/instance/{container}/{kill|stop|start}
 * GET /api/flows/{flow}/failures?window=     failures grouped by stage and message (+ event lifecycle errors)
 * GET  /api/model/info     Model query: legend-lite status, mapped classes (per table) and sample queries
 * POST /api/model/query    {"query": "<Pure lambda>", "stores": "duckdb,clickhouse"} -> per store {sql, columns, rows, ...}
 *                          (legend-lite plans the SQL, LEGEND_LITE_URL; concert runs it read-only)
 * GET  /api/model/store    the generated Legend store layer (Database / Mapping / Connection / Runtime)
 * GET  /api/model/diagrams per machine: state diagram (event + payload class), transition classes, Pure model;
 *                          event catalogs' flow and model diagrams (Model tab Diagrams, Legend Studio's Diagrams panel)
 * GET /api/marketdata/status · POST /api/marketdata/start?rate= · POST /api/marketdata/stop   marketdata-sim control
 * GET /api/recent                 latest finished / dropped events
 * GET /api/events/{eventId}       timeline of one event
 * GET /api/entities/{smType:key}  live state, history, Mermaid state diagram and model class diagram
 * GET /api/locks/{lockKey}        live holder and queue of a lock
 * GET /api/event-types            event-style catalogs (EventCatalog SPI): types, states, flow and model diagrams
 * GET  /api/ops/events?status=errors|ERROR_BLOCKING|ERROR_NON_BLOCKING|SCHEDULED|DONE|all&limit=   lifecycle rows
 * POST /api/ops/events/{eventId}/retry          operator retry of a blocked / parked event
 * POST /api/ops/events/{eventId}/skip?reason=   operator skip (goes DONE without running)
 * GET  /api/ops/events/{eventId}/processor      live status of the event's processor (scheduled, blocked, parked)
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
public final class DashboardMain {
    private static final Logger log = LoggerFactory.getLogger(DashboardMain.class);

    private final FlowRegistry registry;
    private final FlowOps ops;
    private final PerfOps perf;
    private final StateStore store;
    private final WorkflowClient client;
    private final String temporalUi = Env.get("TEMPORAL_UI_URL", "http://localhost:8080");
    /** The DuckDB sink's HTTP API; http://localhost:8090 when the dashboard runs outside compose. */
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
    private final EventsApi events;
    private final FlowMetrics flows;
    private final FlowGenerator generator;
    private final FlowTemporal temporal;
    private final String marketData = Env.get("MARKETDATA_URL", "http://marketdata-sim:8091");
    private final ModelQuery model;
    private final CloudWatchMetrics cloudWatch;
    private final CloudWatchLogs cloudWatchLogs;

    static final List<String> DEEPHAVEN_WIDGETS = List.of("basket_value_chart", "basket_value", "basket_positions", "basket_by_sector",
            "quotes_latest", "vwap_by_symbol", "completions_per_minute",
            "state_distribution", "completion_latency", "revenue_by_currency", "quotes_with_sector", "orders_latest",
            "account_pnl", "pnl_by_account", "fill_slippage", "slippage_by_venue", "vwap_vs_arrival", "sector_exposure");
    static final List<String> DEEPHAVEN_NOTEBOOKS = List.of("01_live_quotes_and_spreads.py", "02_vwap_and_bars.py",
            "03_entity_completions.py", "04_slippage_asof.py", "05_trading_pnl.py", "06_order_basket.py");

    /**
     * Every state machine on the class path, by smType: each module that provides machines implements
     * the {@link MachineCatalog} SPI (sample workers, trading, and every generated ecosystem, which the
     * dashboard build adds as runtime dependencies automatically), so no module is named here.
     */
    static final Map<String, MachineCatalog.Machine> MACHINES = MachineCatalog.all();

    /** Mermaid class diagram of each typed machine's Pure model, by smType. */
    static final Map<String, String> MODEL_DIAGRAMS = modelDiagrams();

    /** State, transition-class and model diagrams of every machine and event catalog ({@link MachineDiagrams}). */
    static final Map<String, Object> DIAGRAMS = MachineDiagrams.all();

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

    DashboardMain(StateStore store, WorkflowClient client) {
        this.store = store;
        this.client = client;
        this.reconcile = new ReconcileRunner(client, MACHINES);
        this.events = new EventsApi(client, store);
        this.registry = FlowRegistry.load(repo());
        this.flows = new FlowMetrics(store, client, pipeline, clickHouse, http, sinkDuckDb, deephavenInternal,
                new KinesisTail(io.concert.orchestration.KinesisClients.fromEnv(), Flows.STREAM_PREFIX), events.types(), events);
        this.flows.ecosystems(id -> {
            FlowRegistry.Flow f = registry.ofMachineFlow(id);
            return f == null ? "" : f.id();
        });
        this.generator = new FlowGenerator(io.concert.orchestration.KinesisClients.fromEnv(), registry::streamOf);
        this.temporal = new FlowTemporal(client, temporalUi);
        DockerCompose compose = new DockerCompose(repo(), registry.composeFiles());
        this.ops = new FlowOps(registry, compose, io.concert.orchestration.KinesisClients.fromEnv(), client, store, generator);
        // CloudWatch: per-machine metrics every 30 s and every flow's container logs (LocalStack's cloudwatch + logs)
        this.cloudWatch = new CloudWatchMetrics(CloudWatchMetrics.client(), flows, id -> {
            FlowRegistry.Flow f = registry.ofMachineFlow(id);
            return f == null ? "" : f.id();
        });
        this.cloudWatchLogs = new CloudWatchLogs(CloudWatchMetrics.logsClient(), compose, registry::all);
        this.ops.cloudWatchStatus(cloudWatch::status);
        this.perf = new PerfOps(ops, generator, flows, openPerfStore());
        this.model = new ModelQuery(Env.get("LEGEND_LITE_URL", "http://legend-lite:8095"), sinkDuckDb, clickHouse, http);
    }

    /** The Performance page's history ({@code PERF_DB}), or {@code null} (runs still work, nothing is kept). */
    private static PerfStore openPerfStore() {
        java.nio.file.Path file = java.nio.file.Path.of(Env.get("PERF_DB", repo().resolve("build/perf/perf-runs.duckdb").toString()));
        try {
            PerfStore s = new PerfStore(file);
            log.info("performance runs are kept in {}", file);
            return s;
        } catch (java.sql.SQLException | RuntimeException e) {
            log.warn("performance runs are not kept: cannot open {}: {}", file, e.getMessage());
            return null;
        }
    }

    /**
     * The repo: {@code CONCERT_REPO} (compose mounts it there, at its host path), else the nearest directory up from
     * the working directory that has a settings.gradle.kts (running from a checkout).
     */
    static java.nio.file.Path repo() {
        String env = Env.get("CONCERT_REPO", "");
        if (!env.isEmpty()) {
            return java.nio.file.Path.of(env);
        }
        java.nio.file.Path p = java.nio.file.Path.of("").toAbsolutePath();
        for (java.nio.file.Path q = p; q != null; q = q.getParent()) {
            if (java.nio.file.Files.isRegularFile(q.resolve("settings.gradle.kts"))) {
                return q;
            }
        }
        return p;
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("STORE_INIT_SCHEMA", Env.get("STORE_INIT_SCHEMA", "false"));
        registerSpecs();
        log.info("state machines: {}", MACHINES.keySet());
        DashboardMain ui = new DashboardMain(Stores.fromEnv(), TemporalClients.fromEnv());
        int port = Env.getInt("DASHBOARD_PORT", Env.getInt("TRACE_UI_PORT", 8088));
        ui.start(port);
        log.info("Concert Dashboard on http://localhost:{}", port);
    }

    HttpServer start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/api/flows", this::flows);
        server.createContext("/api/control/", this::control);
        server.createContext("/api/perf", this::perf);
        server.createContext("/api/cloudwatch/", this::cloudWatch);
        server.createContext("/api/marketdata/", this::marketData);
        server.createContext("/api/model/", this::model);
        server.createContext("/api/recent", ex -> json(ex, Map.of("events", store.recentEvents(50))));
        server.createContext("/api/events/", ex -> json(ex, event(tail(ex, "/api/events/"))));
        server.createContext("/api/entities/", ex -> json(ex, entity(tail(ex, "/api/entities/"))));
        server.createContext("/api/locks/", ex -> json(ex, lock(tail(ex, "/api/locks/"))));
        server.createContext("/api/event-types", ex -> json(ex, events.types()));
        server.createContext("/api/ops/events", this::ops);
        server.createContext("/api/analytics/duckdb/", ex -> proxy(ex, sinkDuckDb, tail(ex, "/api/analytics/duckdb/")));
        server.createContext("/api/analytics/clickhouse/", ex -> clickHouse(ex, tail(ex, "/api/analytics/clickhouse/")));
        server.createContext("/api/analytics/deephaven/config", ex -> json(ex, deephavenConfig()));
        server.createContext("/api/analytics/lag", ex -> json(ex, lag()));
        server.createContext("/api/analytics/reconcile", this::reconcile);
        server.createContext("/", this::staticPage);
        server.start();
        flows.startBackground();
        return server;
    }

    Map<String, Object> event(String eventId) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("eventId", eventId);
        out.put("dedupeStatus", store.processedStatus(eventId).orElse(null));
        out.put("trace", store.traceForEvent(eventId));
        out.putAll(events.event(eventId));
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
        out.put("live", null); // no open lock workflow in any flow: the key is free
        // each flow has its own lock workflows (lock:<flow>:<key>): show the first flow where the key is open
        List<String> ecos = new ArrayList<>(registry.all().stream().map(FlowRegistry.Flow::id).toList());
        ecos.add("");
        String id = Flows.lockWorkflowId(ecos.getFirst(), lockKey);
        for (String eco : ecos) {
            String wf = Flows.lockWorkflowId(eco, lockKey);
            try {
                out.put("live", client.newWorkflowStub(KeyLockWorkflow.class, wf).snapshot());
                out.put("flow", eco);
                id = wf;
                break;
            } catch (RuntimeException idle) {
                // not open in this flow
            }
        }
        out.put("temporalUi", temporalUi + "/namespaces/default/workflows/" + id);
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

    /** {@code /api/flows} (landing page) and {@code /api/flows/{flow}} (one flow's pipeline). */
    private void flows(HttpExchange ex) throws IOException {
        int window;
        try {
            window = Math.max(10, Math.min(KinesisTail.HISTORY_SEC, Integer.parseInt(query(ex).getOrDefault("window", "60"))));
        } catch (NumberFormatException e) {
            window = 60;
        }
        String rest = tail(ex, "/api/flows");
        rest = rest.startsWith("/") ? rest.substring(1) : rest;
        String[] parts = rest.split("/", 2);
        String id = parts[0];
        String action = parts.length > 1 ? parts[1] : "";
        boolean post = ex.getRequestMethod().equals("POST");
        Map<String, String> q = query(ex);
        try {
            if (id.isEmpty()) {
                json(ex, flows.list(window));
                return;
            }
            switch (action) {
                case "" -> {
                    Map<String, Object> view = flows.flowView(id, window);
                    view.put("generator", generator.status(id));
                    view.put("legend", model.status());
                    view.put("studio", model.studioLink());
                    json(ex, view);
                }
                case "generate" -> {
                    if (!post) {
                        json(ex, generator.status(id));
                        return;
                    }
                    // Without a worker, every event waits in a dispatch that holds one of the namespace's long-poll
                    // slots; enough of them starve every other flow's dispatches ("concurrent poller limit exceeded").
                    Integer pollers = flows.workerPollers(id);
                    boolean force = Boolean.parseBoolean(q.getOrDefault("force", "false"));
                    if (pollers == null && !force) {
                        // Unknown is not "fine": Temporal is down or unreachable, and nothing would dispatch what we publish.
                        throw new IllegalStateException("Temporal is not reachable (can't see who polls "
                                + flows.flow(id).taskQueue() + "): generated events would only pile up in Kinesis. "
                                + "Bring Temporal back first (docker compose ps temporal), or pass force=true.");
                    }
                    if (pollers != null && pollers == 0 && !force) {
                        throw new IllegalStateException("no worker polls " + flows.flow(id).taskQueue() + ": generated events would "
                                + "wait in Temporal holding dispatch slots and stall the other flows. Start its worker first"
                                + workerHint(flows.flow(id).group()) + ", or pass force=true.");
                    }
                    json(ex, generator.start(id, Double.parseDouble(q.getOrDefault("rate", "2")),
                            Integer.parseInt(q.getOrDefault("duration", "60")), Boolean.parseBoolean(q.getOrDefault("errors", "false")),
                            Boolean.parseBoolean(q.getOrDefault("noLocks", "false"))));
                }
                case "generate/stop" -> json(ex, generator.stop(id));
                case "temporal" -> {
                    FlowRegistry.Flow eco = registry.ofMachineFlow(id);
                    json(ex, temporal.view(flows.flow(id), flows.lockKeys(id, window), eco == null ? "" : eco.id()));
                }
                case "failures" -> json(ex, flows.failures(id, window));
                case "meta" -> json(ex, flows.meta(id));
                default -> json(ex, 404, Map.of("error", "unknown flow action " + action));
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            json(ex, 409, Map.of("error", String.valueOf(e.getMessage())));
        } catch (RuntimeException e) {
            log.warn("flow metrics failed", e);
            json(ex, 500, Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /** The Performance page ({@link PerfOps}). */
    private void perf(HttpExchange ex) throws IOException {
        String rest = ex.getRequestURI().getPath().substring("/api/perf".length()).replaceFirst("^/", "");
        String[] p = rest.isEmpty() ? new String[0] : rest.split("/");
        boolean post = ex.getRequestMethod().equals("POST");
        Map<String, String> q = query(ex);
        try {
            if (p.length == 0) {
                json(ex, perf.view());
                return;
            }
            if (!post && p[0].equals("runs")) {
                json(ex, perf.history(intParam(q, "limit", 50)));
                return;
            }
            if (!post && p[0].equals("compare")) {
                json(ex, perf.compare(java.util.Arrays.stream(q.getOrDefault("ids", "").split(","))
                        .map(String::trim).filter(x -> !x.isEmpty()).toList()));
                return;
            }
            if (!post) {
                json(ex, 405, Map.of("error", "POST /api/perf/" + rest));
                return;
            }
            json(ex, switch (p[0]) {
                case "start" -> perf.start(q.getOrDefault("flow", ""), q.getOrDefault("preset", "balanced"), q.get("machine"),
                        intParam(q, "coordinators", 0), intParam(q, "workers", 0), Double.parseDouble(q.getOrDefault("rate", "0")),
                        intParam(q, "duration", 300), Boolean.parseBoolean(q.getOrDefault("chaos", "false")),
                        intParam(q, "chaosEvery", 20), Boolean.parseBoolean(q.getOrDefault("chaosKafka", "false")),
                        Boolean.parseBoolean(q.getOrDefault("stopAnalytics", "true")), q.getOrDefault("lockMode", "serial"));
                case "stop" -> perf.stop();
                case "restore" -> perf.restore();
                case "chaos" -> perf.chaos(p.length > 1 && p[1].equals("on"));
                case "runs" -> {
                    if (p.length < 3 || !p[2].equals("delete")) {
                        throw new IllegalArgumentException("POST /api/perf/runs/{runId}/delete");
                    }
                    yield perf.deleteRun(p[1]);
                }
                case "kafka" -> perf.kafka(p.length > 1 ? p[1] : "");
                case "instance" -> {
                    if (p.length < 3) {
                        throw new IllegalArgumentException("POST /api/perf/instance/{container}/{kill|stop|start}");
                    }
                    yield perf.instance(p[1], p[2]);
                }
                default -> throw new IllegalArgumentException("unknown performance operation " + p[0]);
            });
        } catch (IllegalArgumentException | IllegalStateException e) {
            json(ex, 409, Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    private static int intParam(Map<String, String> q, String key, int dflt) {
        String v = q.get(key);
        return v == null || v.isBlank() ? dflt : (int) Double.parseDouble(v);
    }

    /**
     * CloudWatch for one machine flow: {@code GET /api/cloudwatch/metrics/<machine>?window=} (the Metrics tab),
     * {@code GET /api/cloudwatch/logs/<machine>?window=&filter=&streams=a,b&limit=} (the Logs tab: its flow's log group)
     * and {@code GET /api/cloudwatch/status}.
     */
    private void cloudWatch(HttpExchange ex) throws IOException {
        String rest = tail(ex, "/api/cloudwatch/");
        int slash = rest.indexOf('/');
        String what = slash < 0 ? rest : rest.substring(0, slash);
        String machine = slash < 0 ? "" : rest.substring(slash + 1);
        Map<String, String> q = query(ex);
        int window;
        try {
            window = Math.max(60, Math.min(14 * 86_400, Integer.parseInt(q.getOrDefault("window", "900"))));
        } catch (NumberFormatException e) {
            window = 900;
        }
        try {
            switch (what) {
                case "status" -> {
                    Map<String, Object> m = new LinkedHashMap<>(cloudWatch.status());
                    m.put("endpoint", CloudWatchMetrics.endpoint());
                    m.put("namespace", CloudWatchMetrics.NAMESPACE);
                    m.put("publishSec", CloudWatchMetrics.PUBLISH_SEC);
                    m.put("logShipper", cloudWatchLogs.status());
                    json(ex, m);
                }
                case "metrics" -> json(ex, cloudWatch.query(machine, window));
                case "logs" -> {
                    FlowRegistry.Flow f = registry.ofMachineFlow(machine);
                    if (f == null) {
                        f = registry.all().stream().filter(x -> x.id().equals(machine)).findFirst().orElse(null);
                    }
                    String streams = q.getOrDefault("streams", "");
                    int limit = Math.max(10, Math.min(2000, Integer.parseInt(q.getOrDefault("limit", "300"))));
                    Map<String, Object> m = cloudWatchLogs.query(f == null ? "" : f.id(), window,
                            streams.isBlank() ? List.of() : List.of(streams.split(",")), q.getOrDefault("filter", ""), limit);
                    m.put("flow", f == null ? null : f.id());
                    m.put("services", f == null ? List.of() : f.services());
                    json(ex, m);
                }
                default -> json(ex, 404, Map.of("error", "GET /api/cloudwatch/status | metrics/<machine> | logs/<machine>"));
            }
        } catch (RuntimeException e) {
            json(ex, 502, Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /**
     * Flow operations and the shared services:
     * <pre>
     * GET  /api/control/flows                every flow (ecosystem): machines, containers, state, stream / topic / queues, job
     * GET  /api/control/flows/{id}           one flow, with its stream's status
     * POST /api/control/flows/{id}/start|stop|delete    background job (FlowOps); its log is in the flow's "job"
     * GET  /api/control/shared               Kinesis, Kafka, DuckDB, ClickHouse, legend-lite (+ Studio) and their states
     * POST /api/control/shared/start|stop    one switch for all of them
     * </pre>
     */
    private void control(HttpExchange ex) throws IOException {
        String[] p = tail(ex, "/api/control/").split("/");
        boolean post = ex.getRequestMethod().equals("POST");
        try {
            if (p[0].equals("shared")) {
                if (p.length == 1) {
                    json(ex, ops.shared());
                } else if (post && (p[1].equals("start") || p[1].equals("stop"))) {
                    json(ex, ops.shared(p[1].equals("start")));
                } else {
                    json(ex, 404, Map.of("error", "POST /api/control/shared/start|stop"));
                }
                return;
            }
            if (!p[0].equals("flows")) {
                json(ex, 404, Map.of("error", "unknown control " + p[0]));
                return;
            }
            if (p.length == 1 || p[1].isEmpty()) {
                json(ex, ops.flows());
            } else if (p.length == 2) {
                json(ex, ops.flow(p[1]));
            } else if (!post) {
                json(ex, 405, Map.of("error", "POST /api/control/flows/" + p[1] + "/" + p[2]));
            } else {
                json(ex, switch (p[2]) {
                    case "start" -> ops.start(p[1]);
                    case "stop" -> ops.stop(p[1]);
                    case "delete" -> ops.delete(p[1]);
                    default -> throw new IllegalArgumentException("unknown flow operation " + p[2]);
                });
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            json(ex, 409, Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /** How to start the worker of a flow's catalog group. */
    static String workerHint(String group) {
        return switch (group) {
            case "samples", "other" -> "";
            case "trading" -> " (scripts/up.sh <store> --analytics --trading)";
            default -> " (./deploy-concert-ecosystem " + group + ")";
        };
    }

    /** The Model query tab (see {@link ModelQuery}). */
    private void model(HttpExchange ex) throws IOException {
        String op = tail(ex, "/api/model/");
        try {
            switch (op) {
                case "info" -> {
                    String types = query(ex).get("types");
                    json(ex, types == null || types.isBlank() ? model.info()
                            : model.info(new java.util.LinkedHashSet<>(List.of(types.split(",")))));
                }
                case "query" -> {
                    if (!ex.getRequestMethod().equals("POST")) {
                        json(ex, 405, Map.of("error", "POST {\"query\": ..., \"stores\": \"duckdb,clickhouse\"}"));
                        return;
                    }
                    var body = Json.MAPPER.readTree(ex.getRequestBody().readAllBytes());
                    String q = body.path("query").asText("");
                    if (q.isBlank()) {
                        json(ex, 400, Map.of("error", "empty query"));
                        return;
                    }
                    json(ex, model.run(q, body.path("stores").asText("duckdb,clickhouse"), body.path("variant").asText(null)));
                }
                case "diagrams" -> json(ex, DIAGRAMS);
                case "store" -> {
                    byte[] b = model.store().getBytes(StandardCharsets.UTF_8);
                    ex.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
                    ex.sendResponseHeaders(200, b.length);
                    ex.getResponseBody().write(b);
                    ex.close();
                }
                case String s when s.startsWith("studio/") -> {
                    // Legend Studio's "Load demo projects" (legend-studio proxies its projects/ here)
                    String file = s.substring("studio/".length());
                    if (file.equals("manifest.json")) {
                        json(ex, model.studioManifest());
                        return;
                    }
                    String code = model.studioFile(file);
                    if (code == null) {
                        json(ex, 404, Map.of("error", "no such project file " + file));
                        return;
                    }
                    byte[] b = code.getBytes(StandardCharsets.UTF_8);
                    ex.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
                    ex.sendResponseHeaders(200, b.length);
                    ex.getResponseBody().write(b);
                    ex.close();
                }
                default -> json(ex, 404, Map.of("error", "unknown model operation " + op));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            json(ex, 500, Map.of("error", "interrupted"));
        } catch (IOException | RuntimeException e) {
            json(ex, 502, Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /** Forwards status / start / stop to marketdata-sim's control endpoint (MARKETDATA_URL). */
    private void marketData(HttpExchange ex) throws IOException {
        String op = tail(ex, "/api/marketdata/");
        if (!List.of("status", "start", "stop").contains(op)) {
            json(ex, 404, Map.of("error", "unknown market data operation " + op));
            return;
        }
        String raw = ex.getRequestURI().getRawQuery();
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(marketData + "/" + op + (raw == null ? "" : "?" + raw)))
                .timeout(Duration.ofSeconds(3));
        req = op.equals("status") ? req.GET() : req.POST(HttpRequest.BodyPublishers.noBody());
        try {
            HttpResponse<String> r = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
            byte[] b = r.body().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(r.statusCode(), b.length);
            ex.getResponseBody().write(b);
            ex.close();
        } catch (IOException e) {
            json(ex, 502, Map.of("error", "marketdata-sim not reachable at " + marketData + " (start with --analytics)"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            json(ex, 502, Map.of("error", "interrupted"));
        }
    }

    /** The operator API of event-style events (see the class doc). */
    private void ops(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex);
        String rest = tail(ex, "/api/ops/events");
        rest = rest.startsWith("/") ? rest.substring(1) : rest;
        try {
            if (rest.isEmpty()) {
                json(ex, events.list(q.get("status"), Integer.parseInt(q.getOrDefault("limit", "200"))));
                return;
            }
            int slash = rest.lastIndexOf('/');
            String id = slash < 0 ? rest : rest.substring(0, slash);
            String action = slash < 0 ? "" : rest.substring(slash + 1);
            boolean post = ex.getRequestMethod().equals("POST");
            switch (action) {
                case "retry" -> json(ex, post ? events.retry(id) : Map.of("error", "POST to retry"));
                case "skip" -> json(ex, post ? events.skip(id, q.get("reason")) : Map.of("error", "POST to skip"));
                case "processor" -> json(ex, events.processor(id));
                default -> json(ex, 404, Map.of("error", "unknown operation " + action));
            }
        } catch (RuntimeException e) {
            Throwable c = e;
            while (c.getCause() != null && c.getCause() != c) {
                c = c.getCause();
            }
            json(ex, 409, Map.of("error", String.valueOf(c.getMessage())));
        }
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> q = new LinkedHashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw != null) {
            for (String kv : raw.split("&")) {
                int i = kv.indexOf('=');
                q.put(URLDecoder.decode(i < 0 ? kv : kv.substring(0, i), StandardCharsets.UTF_8),
                        i < 0 ? "true" : URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
            }
        }
        return q;
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
        try (InputStream in = DashboardMain.class.getResourceAsStream("/web/index.html")) {
            byte[] bytes = in.readAllBytes();
            ex.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(200, bytes.length);
            ex.getResponseBody().write(bytes);
        }
        ex.close();
    }
}
