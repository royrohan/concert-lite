package io.concert.traceui;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Env;
import io.concert.common.Json;
import io.concert.common.TaskQueues;
import io.concert.common.TraceRow;
import io.concert.sdk.MachineCatalog;
import io.concert.store.StateStore;
import io.temporal.api.enums.v1.TaskQueueType;
import io.temporal.api.taskqueue.v1.TaskQueue;
import io.temporal.api.workflowservice.v1.DescribeTaskQueueRequest;
import io.temporal.client.WorkflowClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The trace UI's flow view: which flows exist (one per entity smType and one per event-style domain,
 * grouped by the catalog that defines them) and, per flow, the metrics of each hop of
 * Publisher → Kinesis → Coordinator → Worker → Kafka → DuckDB / Deephaven / ClickHouse.
 *
 * <ul>
 *   <li>Publisher: records put on the ingest stream per flow ({@link KinesisTail});
 *   <li>Kinesis: arrival rate, transit time to the coordinator ({@code kinesisMs} of RECEIVED rows);
 *   <li>Coordinator: RECEIVED (claimed and dispatched), DROPPED duplicates, lock waits;
 *   <li>Worker: DONE, errors (FAILED / BLOCKED / PARKED), business REJECTED, end-to-end {@code e2eMs};
 *   <li>Kafka: snapshot-topic records/s (all flows), this flow's snapshots/s, consumer lag, DLQ;
 *   <li>Stores: volume of the flow's rows and latency (ClickHouse: completed → ingested; DuckDB: batch
 *       apply time and lag; Deephaven reads the topic from offset 0 and reports neither).
 * </ul>
 *
 * Trace-derived counts come from one windowed read of {@code event_trace}, capped at {@link #TRACE_CAP}
 * rows (newest first): past the cap the rates use the time span the rows cover, and counts are scaled
 * up by {@code TRACE_SAMPLE}.
 */
final class FlowMetrics {
    static final int TRACE_CAP = 20_000;
    static final int SERIES_POINTS = 30;
    private static final Pattern NUM = Pattern.compile("\\b(e2eMs|kinesisMs|dedupeMs|waitedMs)=(-?\\d+)");
    private static final Set<TraceRow.Stage> ERRORS = Set.of(TraceRow.Stage.FAILED, TraceRow.Stage.BLOCKED, TraceRow.Stage.PARKED);

    /** A flow the landing page offers. {@code snapshotTypes}: sm_type values of its rows in the sinks. */
    record Flow(String id, String label, String style, String group, List<String> snapshotTypes, String taskQueue) {}

    private final StateStore store;
    private final WorkflowClient client;
    private final PipelineStatus pipeline;
    private final ClickHouseConsole clickHouse;
    private final HttpClient http;
    private final String sinkDuckDb;
    private final String deephavenInternal;
    private final KinesisTail tail;
    /** A machine flow's flow (ecosystem): its stream, topic and orchestration queue ({@link io.concert.common.Flows}). */
    private volatile java.util.function.Function<String, String> ecosystemOf = id -> "";
    private final double sample = Math.max(0.0001, Math.min(1, Double.parseDouble(Env.get("TRACE_SAMPLE", "1.0"))));
    private final Map<String, Flow> flows;
    private final Map<String, Deque<long[]>> counters = new HashMap<>();
    private final Map<String, Object[]> cache = new HashMap<>();
    private final EventsApi events;

    FlowMetrics(StateStore store, WorkflowClient client, PipelineStatus pipeline, ClickHouseConsole clickHouse, HttpClient http,
            String sinkDuckDb, String deephavenInternal, KinesisTail tail, Map<String, Object> eventTypes, EventsApi events) {
        this.store = store;
        this.client = client;
        this.pipeline = pipeline;
        this.clickHouse = clickHouse;
        this.http = http;
        this.sinkDuckDb = sinkDuckDb;
        this.deephavenInternal = deephavenInternal;
        this.tail = tail;
        this.events = events;
        this.flows = catalog(MachineCatalog.load(), eventTypes);
    }

    void ecosystems(java.util.function.Function<String, String> ecosystemOf) {
        this.ecosystemOf = ecosystemOf;
    }

    private String eco(String machineFlow) {
        String e = ecosystemOf.apply(machineFlow);
        return e == null ? "" : e;
    }

    private String topicOf(String machineFlow) {
        return io.concert.common.Flows.snapshotTopic(eco(machineFlow));
    }

    // ---------------------------------------------------------------- catalog

    /** Entity flows per machine catalog, then event flows per event-catalog domain, in catalog order. */
    static Map<String, Flow> catalog(List<MachineCatalog> machines, Map<String, Object> eventTypes) {
        Map<String, Flow> out = new LinkedHashMap<>();
        for (MachineCatalog c : machines) {
            for (MachineCatalog.Machine m : c.machines()) {
                out.putIfAbsent(m.smType(), new Flow(m.smType(), m.smType(), "entity", c.name(), List.of(m.smType()),
                        TaskQueues.stateMachine(m.smType())));
            }
        }
        JsonNode cats = Json.MAPPER.valueToTree(eventTypes).path("catalogs");
        Map<String, Set<String>> byDomain = new LinkedHashMap<>();
        Map<String, String> groupOf = new HashMap<>();
        for (JsonNode c : cats) {
            String name = c.path("name").asText();
            String group = name.contains("/") ? name.substring(0, name.indexOf('/')) : name;
            for (JsonNode e : c.path("events")) {
                String domain = e.path("domain").asText();
                byDomain.computeIfAbsent(domain, d -> new LinkedHashSet<>()).add("evt_" + snake(e.path("name").asText()));
                groupOf.putIfAbsent(domain, group);
            }
        }
        byDomain.forEach((domain, types) -> out.putIfAbsent("ev:" + domain,
                new Flow("ev:" + domain, domain, "event", groupOf.get(domain), List.copyOf(types), TaskQueues.events(domain))));
        return out;
    }

    static String snake(String camel) {
        return camel.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
    }

    /** The flow of a trace row: {@code ev:<domain>} for processor ids {@code evproc:<domain>:<key>}, else the smType. */
    static String flowOf(String workflowId) {
        if (workflowId == null || workflowId.isEmpty()) {
            return null;
        }
        if (workflowId.startsWith("evproc:")) {
            int end = workflowId.indexOf(':', 7);
            return "ev:" + (end < 0 ? workflowId.substring(7) : workflowId.substring(7, end));
        }
        int colon = workflowId.indexOf(':');
        return colon < 0 ? workflowId : workflowId.substring(0, colon);
    }

    Flow flow(String id) {
        Flow f = flows.get(id);
        if (f != null) {
            return f;
        }
        boolean ev = id.startsWith("ev:");
        return new Flow(id, ev ? id.substring(3) : id, ev ? "event" : "entity", "other", ev ? List.of() : List.of(id),
                ev ? TaskQueues.events(id.substring(3)) : TaskQueues.stateMachine(id));
    }

    // ---------------------------------------------------------------- trace aggregation

    /** Per-flow counts and timings over one window of trace rows. */
    static final class Agg {
        final Map<TraceRow.Stage, Long> stages = new EnumMap<>(TraceRow.Stage.class);
        final List<Long> e2e = new ArrayList<>();
        final List<Long> kinesis = new ArrayList<>();
        final List<Long> dedupe = new ArrayList<>();
        final List<Long> lockWait = new ArrayList<>();
        final long[] doneSeries = new long[SERIES_POINTS];
        final long[] receivedSeries = new long[SERIES_POINTS];
        final long[] errorSeries = new long[SERIES_POINTS];
        long rejectedDone;
        long lastSeen;
        final List<TraceRow> recent = new ArrayList<>();
        final List<TraceRow> errors = new ArrayList<>();
        /** Lock keys the flow's events waited on, newest first (the coordinator page's queues). */
        final Set<String> lockKeys = new LinkedHashSet<>();
        /** Every failure and rejection in the window, newest first (the worker page groups them). */
        final List<TraceRow> failures = new ArrayList<>();

        long count(TraceRow.Stage s) {
            return stages.getOrDefault(s, 0L);
        }

        long errorCount() {
            return ERRORS.stream().mapToLong(this::count).sum();
        }
    }

    /** Window of trace rows aggregated per flow: {@code (aggs, coveredMs, truncated)}. */
    record Window(Map<String, Agg> aggs, long coveredMs, boolean truncated, long fromMillis) {}

    Window window(int windowSec, String onlyFlow) {
        long now = System.currentTimeMillis();
        long from = now - windowSec * 1000L;
        List<TraceRow> rows = store.tracesSince(from, TRACE_CAP);
        boolean truncated = rows.size() >= TRACE_CAP;
        long start = truncated ? rows.get(rows.size() - 1).tsMillis() : from;
        long covered = Math.max(1000, now - start);
        long step = Math.max(1, covered / SERIES_POINTS);
        Map<String, Agg> aggs = new TreeMap<>();
        for (TraceRow r : rows) {
            String flow = flowOf(r.workflowId());
            if (flow == null || (onlyFlow != null && !onlyFlow.equals(flow))) {
                continue;
            }
            Agg a = aggs.computeIfAbsent(flow, f -> new Agg());
            a.stages.merge(r.stage(), 1L, Long::sum);
            a.lastSeen = Math.max(a.lastSeen, r.tsMillis());
            int idx = (int) Math.max(0, Math.min(SERIES_POINTS - 1, (r.tsMillis() - start) / step));
            Map<String, Long> nums = numbers(r.detail());
            switch (r.stage()) {
                case RECEIVED -> {
                    a.receivedSeries[idx]++;
                    addIf(a.kinesis, nums.get("kinesisMs"));
                    addIf(a.dedupe, nums.get("dedupeMs"));
                }
                case DONE -> {
                    a.doneSeries[idx]++;
                    addIf(a.e2e, nums.get("e2eMs"));
                    if (r.detail() != null && r.detail().startsWith("rejected")) {
                        a.rejectedDone++;
                    }
                }
                case LOCK_GRANTED -> addIf(a.lockWait, nums.get("waitedMs"));
                case FAILED, BLOCKED, PARKED -> a.errorSeries[idx]++;
                default -> { }
            }
            boolean terminal = switch (r.stage()) {
                case DONE, DROPPED, FAILED, REJECTED, BLOCKED, PARKED -> true;
                default -> false;
            };
            if (terminal && a.recent.size() < 50) {
                a.recent.add(r);
            }
            if (ERRORS.contains(r.stage()) || r.stage() == TraceRow.Stage.REJECTED) {
                if (a.errors.size() < 25) {
                    a.errors.add(r);
                }
                if (a.failures.size() < 2000) {
                    a.failures.add(r);
                }
            }
            if (r.stage() == TraceRow.Stage.LOCK_WAIT && r.lockKey() != null && a.lockKeys.size() < 150) {
                a.lockKeys.add(r.lockKey());
            }
        }
        return new Window(aggs, covered, truncated, start);
    }

    private static Map<String, Long> numbers(String detail) {
        if (detail == null || detail.indexOf('=') < 0) {
            return Map.of();
        }
        Map<String, Long> out = new HashMap<>(4);
        Matcher m = NUM.matcher(detail);
        while (m.find()) {
            out.put(m.group(1), Long.parseLong(m.group(2)));
        }
        return out;
    }

    private static void addIf(List<Long> xs, Long v) {
        if (v != null && v >= 0) {
            xs.add(v);
        }
    }

    static Long percentile(List<Long> xs, double p) {
        if (xs.isEmpty()) {
            return null;
        }
        List<Long> s = new ArrayList<>(xs);
        s.sort(null);
        return s.get((int) Math.min(s.size() - 1, Math.floor(p * s.size())));
    }

    private long scaled(long sampledCount) {
        return Math.round(sampledCount / sample);
    }

    private Double perSec(long count, long coveredMs) {
        return Math.round(count * 10_000.0 / coveredMs) / 10.0;
    }

    private long[] scaled(long[] series) {
        long[] out = new long[series.length];
        for (int i = 0; i < series.length; i++) {
            out[i] = scaled(series[i]);
        }
        return out;
    }

    // ---------------------------------------------------------------- landing page

    /** {@code {windowSec, coveredSec, traceSample, truncated, kinesis: {...}, groups: [{name, flows: [...]}]}} */
    Map<String, Object> list(int windowSec) {
        Window w = window(windowSec, null);
        Map<String, Object> pub = tail.published(windowSec, SERIES_POINTS);
        Map<String, Long> published = longMap(pub.get("byFlow"));
        long pubCovered = ((Number) pub.get("coveredSec")).longValue() * 1000;

        Set<String> ids = new LinkedHashSet<>(flows.keySet());
        ids.addAll(w.aggs().keySet());
        published.keySet().stream().filter(f -> !f.equals("unknown")).forEach(ids::add);
        Map<String, List<Map<String, Object>>> groups = new LinkedHashMap<>();
        for (String id : ids) {
            Flow f = flow(id);
            Agg a = w.aggs().getOrDefault(id, new Agg());
            long pubCount = published.getOrDefault(id, 0L);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", f.id());
            m.put("label", f.label());
            m.put("style", f.style());
            m.put("published", pubCount);
            m.put("publishedPerSec", perSec(pubCount, pubCovered));
            m.put("dispatched", scaled(a.count(TraceRow.Stage.RECEIVED)));
            m.put("processed", scaled(a.count(TraceRow.Stage.DONE)));
            m.put("processedPerSec", perSec(scaled(a.count(TraceRow.Stage.DONE)), w.coveredMs()));
            m.put("errored", scaled(a.errorCount()));
            m.put("rejected", scaled(a.count(TraceRow.Stage.REJECTED)));
            m.put("e2eP95", percentile(a.e2e, 0.95));
            m.put("lastSeen", a.lastSeen == 0 ? null : a.lastSeen);
            m.put("series", scaled(a.doneSeries));
            groups.computeIfAbsent(f.group(), g -> new ArrayList<>()).add(m);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("windowSec", windowSec);
        out.put("coveredSec", w.coveredMs() / 1000);
        out.put("truncated", w.truncated());
        out.put("traceSample", sample);
        out.put("kinesis", Map.of("stream", pub.get("stream"), "error", String.valueOf(pub.getOrDefault("error", ""))));
        List<Map<String, Object>> gs = new ArrayList<>();
        groups.forEach((name, fs) -> gs.add(Map.of("name", name, "flows", fs)));
        out.put("groups", gs);
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Long> longMap(Object o) {
        Map<String, Long> out = new HashMap<>();
        if (o instanceof Map<?, ?> m) {
            m.forEach((k, v) -> out.put(String.valueOf(k), ((Number) v).longValue()));
        }
        return out;
    }

    // ---------------------------------------------------------------- one flow

    /** Metrics of every hop for one flow (see the class doc), plus its recent and failed events. */
    Map<String, Object> flowView(String id, int windowSec) {
        Flow f = flow(id);
        Window w = window(windowSec, id);
        Agg a = w.aggs().getOrDefault(id, new Agg());
        long covered = w.coveredMs();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("flow", f);
        out.put("windowSec", windowSec);
        out.put("coveredSec", covered / 1000);
        out.put("truncated", w.truncated());
        out.put("traceSample", sample);

        // Publisher + Kinesis
        Map<String, Object> pub = tail.published(windowSec, SERIES_POINTS);
        long pubCovered = ((Number) pub.get("coveredSec")).longValue() * 1000;
        long published = longMap(pub.get("byFlow")).getOrDefault(id, 0L);
        Map<String, Object> publisher = new LinkedHashMap<>();
        publisher.put("published", published);
        publisher.put("perSec", perSec(published, pubCovered));
        publisher.put("coveredSec", pubCovered / 1000);
        publisher.put("series", ((Map<?, ?>) pub.get("series")).get(id));
        publisher.put("error", pub.get("error"));
        out.put("publisher", publisher);

        long dispatched = scaled(a.count(TraceRow.Stage.RECEIVED));
        Map<String, Object> kinesis = new LinkedHashMap<>();
        String stream = io.concert.common.Flows.stream(eco(id));
        kinesis.put("stream", stream);
        kinesis.put("shards", ((Map<?, ?>) pub.getOrDefault("shardsByStream", Map.of())).get(stream));
        kinesis.put("perSec", perSec(published, pubCovered));
        kinesis.put("transitP50", percentile(a.kinesis, 0.5));
        kinesis.put("transitP95", percentile(a.kinesis, 0.95));
        kinesis.put("error", pub.get("error"));
        out.put("kinesis", kinesis);

        // Coordinator
        Map<String, Object> coord = new LinkedHashMap<>();
        coord.put("dispatched", dispatched);
        coord.put("perSec", perSec(dispatched, covered));
        coord.put("duplicates", scaled(a.count(TraceRow.Stage.DROPPED)));
        coord.put("lockWaits", scaled(a.count(TraceRow.Stage.LOCK_WAIT)));
        coord.put("lockWaitP95", percentile(a.lockWait, 0.95));
        coord.put("dedupeP50", percentile(a.dedupe, 0.5));
        coord.put("taskQueue", io.concert.common.Flows.orchestrationQueue(eco(id)));
        coord.put("pollers", pollers(io.concert.common.Flows.orchestrationQueue(eco(id))));
        coord.put("series", scaled(a.receivedSeries));
        out.put("coordinator", coord);

        // Worker
        long processed = scaled(a.count(TraceRow.Stage.DONE));
        Map<String, Object> worker = new LinkedHashMap<>();
        worker.put("taskQueue", f.taskQueue());
        worker.put("pollers", pollers(f.taskQueue()));
        worker.put("processed", processed);
        worker.put("perSec", perSec(processed, covered));
        worker.put("errored", scaled(a.errorCount()));
        worker.put("errors", Map.of("FAILED", scaled(a.count(TraceRow.Stage.FAILED)), "BLOCKED", scaled(a.count(TraceRow.Stage.BLOCKED)),
                "PARKED", scaled(a.count(TraceRow.Stage.PARKED))));
        worker.put("rejected", scaled(a.count(TraceRow.Stage.REJECTED)));
        worker.put("emitted", scaled(a.count(TraceRow.Stage.EMITTED)));
        worker.put("e2eP50", percentile(a.e2e, 0.5));
        worker.put("e2eP95", percentile(a.e2e, 0.95));
        worker.put("series", scaled(a.doneSeries));
        worker.put("errorSeries", scaled(a.errorSeries));
        out.put("worker", worker);

        // Kafka + stores
        Map<String, Object> snap = clickHouseFlow(f, windowSec);
        out.put("kafka", kafka(windowSec, snap, topicOf(id)));
        Map<String, Object> stores = new LinkedHashMap<>();
        stores.put("duckdb", duckDb(f, windowSec));
        stores.put("deephaven", deephaven(topicOf(id)));
        stores.put("clickhouse", snap);
        out.put("stores", stores);

        out.put("recent", a.recent);
        out.put("errors", a.errors);
        return out;
    }

    /**
     * What the analytics pages need to scope themselves to a flow: the flow, its DuckDB tables, and the Deephaven
     * widgets that show it. Widget names follow concert.py and the generated ecosystem snippets: the sample roots
     * ({@code orders_latest}), the trading views ({@code trading_orders}, plus market-data widgets), and per ecosystem
     * {@code <eco>_<table>_latest} or, for event domains, {@code <eco>_events_latest} and friends. An ecosystem's
     * widgets exist only while it is deployed (its compose override loads its snippet into Deephaven).
     */
    Map<String, Object> meta(String id) {
        Flow f = flow(id);
        List<String> tables = cached("duckdb:tables:" + f.id(), 30_000, () -> duckDbTables(f));
        String root = tables.isEmpty() ? f.id() + "s" : tables.getFirst();
        List<String> widgets = new ArrayList<>();
        String eco = f.group().replace('-', '_');
        if ("event".equals(f.style())) {
            widgets.addAll(List.of(eco + "_events_latest", eco + "_events_by_status", eco + "_event_errors", eco + "_events_scheduled"));
        } else if (f.group().equals("samples")) {
            widgets.addAll(List.of(root + "_latest", "state_distribution", "completion_latency", "completions_per_minute"));
        } else if (f.group().equals("trading")) {
            widgets.addAll(List.of(root, "quotes_latest", "vwap_by_symbol", "fill_slippage", "slippage_by_venue", "vwap_vs_arrival", "account_pnl"));
        } else {
            widgets.addAll(List.of(eco + "_" + root + "_latest", eco + "_state_distribution", eco + "_completions_per_minute"));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("flow", f);
        out.put("duckdbTables", tables);
        out.put("deephavenWidgets", widgets);
        // The order basket marked to the live mid first, then the quotes it's marked to.
        out.put("marketDataWidgets", List.of("basket_value_chart", "basket_value", "basket_positions", "basket_by_sector",
                "quotes_latest", "vwap_by_symbol", "quotes_with_sector", "sector_moves"));
        return out;
    }

    /** Lock keys the flow's events waited on in the last {@code windowSec} seconds, newest first. */
    Set<String> lockKeys(String id, int windowSec) {
        Agg a = window(windowSec, id).aggs().get(id);
        return a == null ? Set.of() : a.lockKeys;
    }

    private static final Pattern HEXISH = Pattern.compile("\\b[0-9a-fA-F-]{8,}\\b");
    private static final Pattern DIGITS = Pattern.compile("\\d+");

    /** A failure message with ids and numbers masked, so the same failure on different events groups together. */
    static String normalize(String detail) {
        if (detail == null || detail.isBlank()) {
            return "(no detail)";
        }
        String d = DIGITS.matcher(HEXISH.matcher(detail.strip()).replaceAll("…")).replaceAll("#");
        return d.length() > 160 ? d.substring(0, 160) + "…" : d;
    }

    /**
     * Failures of a flow for the worker page: trace rows (FAILED, BLOCKED, PARKED, REJECTED) in the window and, for an
     * event flow, the lifecycle rows still in ERROR_BLOCKING / ERROR_NON_BLOCKING whenever they failed, grouped by
     * stage and normalized message: {@code {groups: [{key, stage, message, count, firstSeen, lastSeen}], events, lifecycle}}.
     */
    Map<String, Object> failures(String id, int windowSec) {
        Flow f = flow(id);
        Agg a = window(windowSec, id).aggs().getOrDefault(id, new Agg());
        Map<String, Map<String, Object>> groups = new LinkedHashMap<>();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (TraceRow r : a.failures) {
            String msg = normalize(r.detail());
            String key = r.stage().name() + "|" + msg;
            group(groups, key, r.stage().name(), msg, r.tsMillis());
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("eventId", r.eventId());
            m.put("tsMillis", r.tsMillis());
            m.put("stage", r.stage().name());
            m.put("workflowId", r.workflowId());
            m.put("detail", r.detail());
            m.put("group", key);
            rows.add(m);
        }
        List<Map<String, Object>> lifecycle = new ArrayList<>();
        if ("event".equals(f.style()) && events != null) {
            Object list = events.list("errors", 1000).get("events");
            if (list instanceof List<?> l) {
                for (Object o : l) {
                    if (o instanceof io.concert.orchestration.events.EventOps.EventRow e && id.equals("ev:" + e.domain())) {
                        String msg = normalize(e.error());
                        String key = e.status().name() + "|" + msg;
                        group(groups, key, e.status().name(), msg, e.updatedAtMillis());
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("eventId", e.eventId());
                        m.put("eventType", e.eventType());
                        m.put("status", e.status().name());
                        m.put("error", e.error());
                        m.put("attempts", e.attempts());
                        m.put("keys", e.keys());
                        m.put("updatedAtMillis", e.updatedAtMillis());
                        m.put("group", key);
                        lifecycle.add(m);
                    }
                }
            }
        }
        List<Map<String, Object>> gs = new ArrayList<>(groups.values());
        gs.sort(Comparator.comparingLong((Map<String, Object> g) -> (long) g.get("count")).reversed());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("flow", f);
        out.put("windowSec", windowSec);
        out.put("groups", gs);
        out.put("events", rows);
        out.put("lifecycle", lifecycle);
        out.put("truncated", a.failures.size() >= 2000);
        return out;
    }

    private static void group(Map<String, Map<String, Object>> groups, String key, String stage, String msg, long ts) {
        Map<String, Object> g = groups.computeIfAbsent(key, k -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("key", k);
            m.put("stage", stage);
            m.put("message", msg);
            m.put("count", 0L);
            m.put("firstSeen", ts);
            m.put("lastSeen", ts);
            return m;
        });
        g.put("count", (long) g.get("count") + 1);
        g.put("firstSeen", Math.min((long) g.get("firstSeen"), ts));
        g.put("lastSeen", Math.max((long) g.get("lastSeen"), ts));
    }

    /** Workers polling the flow's task queue (sm-* / ev-*), or null if Temporal didn't say. */
    Integer workerPollers(String id) {
        return pollers(flow(id).taskQueue());
    }

    private Integer pollers(String taskQueue) {
        return cached("pollers:" + taskQueue, 5000, () -> {
            try {
                var resp = client.getWorkflowServiceStubs().blockingStub().withDeadlineAfter(2, TimeUnit.SECONDS)
                        .describeTaskQueue(DescribeTaskQueueRequest.newBuilder().setNamespace(client.getOptions().getNamespace())
                                .setTaskQueue(TaskQueue.newBuilder().setName(taskQueue).build())
                                .setTaskQueueType(TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW).build());
                return resp.getPollersCount();
            } catch (RuntimeException e) {
                return null;
            }
        });
    }

    private Map<String, Object> kafka(int windowSec, Map<String, Object> snap, String topic) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("topic", topic);
        long end = cached("kafka:end:" + topic, 1000, () -> pipeline.endOffsets(topic));
        if (end < 0) {
            out.put("error", "topic " + topic + " not readable: Kafka is down or the flow was never started");
            return out;
        }
        out.put("records", end);
        out.put("perSec", rate("kafka:" + topic, end, windowSec));
        out.put("flowPerSec", snap.get("perSec"));
        Map<String, Object> lag = cached("kafka:lag", 3000, pipeline::lag);
        Map<String, Long> byTarget = new TreeMap<>();
        if (lag.get("groups") instanceof List<?> gs) {
            for (Object g : gs) {
                if (g instanceof Map<?, ?> m && m.get("topics") instanceof Map<?, ?> t && t.get(topic) instanceof Map<?, ?> tl) {
                    byTarget.merge(String.valueOf(m.get("target")), ((Number) tl.get("lag")).longValue(), Long::sum);
                }
            }
        }
        out.put("lagByTarget", byTarget);
        Map<String, Object> dlq = cached("kafka:dlq", 5000, pipeline::deadLetters);
        out.put("dlq", dlq.getOrDefault("total", 0L));
        return out;
    }

    /** DuckDB: rows in the flow's tables, sink-wide applied/s, batch latency and consumer lag. */
    private Map<String, Object> duckDb(Flow f, int windowSec) {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            JsonNode h = getJson(sinkDuckDb + "/health");
            long applied = h.path("target").path("appliedEntities").asLong();
            out.put("applied", applied);
            out.put("perSec", rate("duckdb:applied", applied, windowSec));
            out.put("latencyP50", h.path("batchLatencyMs").path("p50").asDouble());
            out.put("latencyP99", h.path("batchLatencyMs").path("p99").asDouble());
            out.put("lag", h.path("consumer").path("lag").asLong());
            out.put("dlq", h.path("dlq").asLong());
            String lastError = h.path("consumer").path("lastError").asText(null);
            if (lastError != null && !lastError.isBlank()) {
                out.put("lastError", lastError);
            }
            List<String> tables = cached("duckdb:tables:" + f.id(), 30_000, () -> duckDbTables(f));
            out.put("tables", tables);
            if (!tables.isEmpty()) {
                StringBuilder sql = new StringBuilder("SELECT ");
                for (int i = 0; i < tables.size(); i++) {
                    sql.append(i == 0 ? "" : " + ").append("(SELECT count(*) FROM \"").append(tables.get(i)).append("\")");
                }
                JsonNode r = postJson(sinkDuckDb + "/query", sql.append(" AS n").toString());
                out.put("volume", r.path("rows").path(0).path(0).asLong());
            }
        } catch (Exception e) {
            out.put("error", "DuckDB sink not reachable");
        }
        return out;
    }

    private List<String> duckDbTables(Flow f) {
        try {
            List<String> out = new ArrayList<>();
            Set<String> eventTables = new LinkedHashSet<>();
            f.snapshotTypes().forEach(t -> eventTables.add(t + "s"));
            for (JsonNode t : getJson(sinkDuckDb + "/tables").path("tables")) {
                String name = t.path("name").asText();
                boolean root = "root".equals(t.path("kind").asText()) && f.id().equals(t.path("smType").asText());
                if (root || ("event".equals(f.style()) && eventTables.contains(name))) {
                    out.add(name);
                }
            }
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    /** ClickHouse: entities of the flow, snapshots ingested in the window and completed → ingested latency. */
    private Map<String, Object> clickHouseFlow(Flow f, int windowSec) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (f.snapshotTypes().isEmpty()) {
            out.put("error", "no snapshot types for this flow");
            return out;
        }
        String types = String.join(",", f.snapshotTypes().stream().map(t -> "'" + t.replace("'", "") + "'").toList());
        String sql = "SELECT uniqExact(entity_id), countIf(ingested_at >= now64(3) - INTERVAL " + windowSec + " SECOND),"
                + " quantileIf(0.5)(dateDiff('millisecond', completed_at, ingested_at), ingested_at >= now64(3) - INTERVAL " + windowSec + " SECOND),"
                + " quantileIf(0.95)(dateDiff('millisecond', completed_at, ingested_at), ingested_at >= now64(3) - INTERVAL " + windowSec + " SECOND),"
                + " quantile(0.5)(dateDiff('millisecond', completed_at, ingested_at)), max(ingested_at)"
                + " FROM entity_snapshots WHERE sm_type IN (" + types + ")";
        try {
            Map<String, Object> r = clickHouse.query(sql);
            // Rows are the JSONCompact arrays as Jackson nodes; 64-bit integers may arrive quoted.
            JsonNode row = Json.MAPPER.valueToTree(((List<?>) r.get("rows")).get(0));
            long inWindow = row.path(1).asLong();
            out.put("volume", row.path(0).asLong());
            out.put("ingested", inWindow);
            out.put("perSec", Math.round(inWindow * 10.0 / windowSec) / 10.0);
            boolean recent = inWindow > 0;
            out.put("latencyP50", recent ? nullableNum(row.path(2)) : null);
            out.put("latencyP95", recent ? nullableNum(row.path(3)) : null);
            out.put("latencyP50AllTime", nullableNum(row.path(4)));
            out.put("lastIngested", row.path(5).asText(null));
            Map<String, Object> h = cached("clickhouse:health", 5000, () -> {
                try {
                    return clickHouse.health();
                } catch (ClickHouseConsole.QueryException e) {
                    return Map.<String, Object>of();
                }
            });
            out.put("kafkaErrors", h.get("parseErrors"));
        } catch (ClickHouseConsole.QueryException | RuntimeException e) {
            out.put("error", "ClickHouse not reachable");
        }
        return out;
    }

    private Map<String, Object> deephaven(String topic) {
        Map<String, Object> out = new LinkedHashMap<>();
        boolean up = cached("deephaven:up", 5000, () -> pipeline.reachable(deephavenInternal + "/ide/"));
        out.put("reachable", up);
        long end = cached("kafka:end:" + topic, 1000, () -> pipeline.endOffsets(topic));
        if (end >= 0) {
            out.put("volume", end);
        }
        out.put("note", "re-reads the topic from offset 0 on start, commits no offsets: no lag or latency to report");
        return out;
    }

    // ---------------------------------------------------------------- plumbing

    private static Long nullableNum(JsonNode n) {
        if (n == null || n.isNull() || n.isMissingNode()) {
            return null;
        }
        double d = n.asDouble(Double.NaN);
        return Double.isNaN(d) ? null : Math.round(d);
    }

    /**
     * Rate of a monotonically increasing counter over (up to) the last {@code windowSec} seconds, from
     * samples recorded at each call; {@code null} until two samples a second apart exist.
     */
    synchronized Double rate(String key, long value, int windowSec) {
        long now = System.currentTimeMillis();
        Deque<long[]> d = counters.computeIfAbsent(key, k -> new ArrayDeque<>());
        long[] last = d.peekLast();
        if (last != null && value < last[1]) {
            d.clear(); // counter reset (sink restart)
        }
        if (last == null || now - last[0] >= 500) {
            d.addLast(new long[] {now, value});
        }
        while (d.size() > 2 && d.peekFirst()[0] < now - KinesisTail.HISTORY_SEC * 1000L) {
            d.pollFirst();
        }
        long[] base = null;
        for (long[] s : d) {
            if (s[0] >= now - windowSec * 1000L) {
                base = s;
                break;
            }
        }
        if (base == null || now - base[0] < 1000) {
            // Fewer than two samples inside the window: use the oldest one we have, if it's recent enough to be fair.
            base = d.peekFirst();
            if (base == null || now - base[0] < 1000) {
                return null;
            }
        }
        return Math.round((value - base[1]) * 10_000.0 / (now - base[0])) / 10.0;
    }

    @SuppressWarnings("unchecked")
    private synchronized <T> T cached(String key, long ttlMs, Supplier<T> load) {
        Object[] hit = cache.get(key);
        long now = System.currentTimeMillis();
        if (hit != null && now - (long) hit[0] < ttlMs) {
            return (T) hit[1];
        }
        T v = load.get();
        cache.put(key, new Object[] {now, v});
        return v;
    }

    private JsonNode getJson(String url) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(2)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        return Json.MAPPER.readTree(r.body());
    }

    private JsonNode postJson(String url, String body) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5))
                .header("Content-Type", "text/plain; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(), HttpResponse.BodyHandlers.ofString());
        return Json.MAPPER.readTree(r.body());
    }

    /**
     * Starts the Kinesis tail and a 2-second sampler of the Kafka topic's end offsets and the DuckDB sink's
     * applied counter, so published counts and rates have history before anyone opens the page.
     */
    void startBackground() {
        tail.ensureStarted();
        Thread.ofVirtual().name("flow-sampler").start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    for (String topic : flows.keySet().stream().map(this::topicOf).distinct().toList()) {
                        long end = pipeline.endOffsets(topic);
                        if (end >= 0) {
                            rate("kafka:" + topic, end, 60);
                        }
                    }
                    long applied = getJson(sinkDuckDb + "/health").path("target").path("appliedEntities").asLong(-1);
                    if (applied >= 0) {
                        rate("duckdb:applied", applied, 60);
                    }
                } catch (Exception e) {
                    // a sink or Kafka that is down: the view reports it; keep sampling
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
    }

    Collection<Flow> flows() {
        return flows.values();
    }
}
