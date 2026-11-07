package io.concert.dashboard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.model.runtime.ModelJson;
import io.concert.sdk.LockTemplates;
import io.concert.sdk.MachineCatalog;
import io.concert.sdk.StateMachineSpec;
import io.concert.sdk.events.EventCatalog;
import io.concert.sdk.events.EventCatalogs;
import io.concert.trading.TradingFlowGenerator;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

/**
 * "Generate data flow" on a flow page: publishes test events for one flow to the ingest stream at a given
 * rate of new instances per second, for a duration, until stopped. How events are made, per flow:
 *
 * <ul>
 *   <li><b>samples</b>: the showcases' sample flows ({@code samples/flows/*.json} for an entity flow,
 *       {@code samples/event-flows/*.json} for an event domain), replayed per instance with their identities
 *       (entity instance keys; event lock-key values such as {@code orderId}, {@code sku}) suffixed, so each
 *       instance is new. Error injection mixes in the non-happy samples (poison, rejected, out of stock).
 *   <li><b>trading</b>: {@link TradingFlowGenerator} order flow (submit → ack → route → fills → allocate → close),
 *       with a higher reject rate when errors are injected.
 *   <li><b>walk</b>: any other entity machine: the shortest path from its initial state to a terminal state, with
 *       payloads left to the machine's defaults; error injection sends an event that is invalid in the
 *       initial state.
 * </ul>
 *
 * Events of one instance go out in order, a step apart; instances interleave. Entity lock keys come from the
 * catalog's {@code LOCKS} (generated ecosystems), else the entity key alone.
 */
final class FlowGenerator implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FlowGenerator.class);
    static final long STEP_MS = 300;
    private static final Set<String> TRADING = Set.of("trading_order", "trading_execution", "trading_fill", "trading_allocation");

    private final KinesisClient kinesis;
    /** A flow page's flow (smType or {@code ev:<domain>}) to its flow's ingest stream ({@code concert-<flow>}). */
    private final java.util.function.Function<String, String> streamOf;
    private final Map<String, Run> runs = new ConcurrentHashMap<>();
    private final Map<String, MachineCatalog.Machine> machines = new HashMap<>();
    private final Map<String, Map<String, List<String>>> entityLocks = new HashMap<>();
    private final Map<String, EventCatalog.EventType> eventTypes = new HashMap<>();
    private final List<Sample> samples;

    /** One sample flow file: its name, whether it's an error case, and its events. */
    record Sample(String path, String name, boolean error, List<JsonNode> events) {}

    FlowGenerator(KinesisClient kinesis, java.util.function.Function<String, String> streamOf) {
        this.kinesis = kinesis;
        this.streamOf = streamOf;
        for (MachineCatalog c : MachineCatalog.load()) {
            Map<String, Map<String, List<String>>> locks = locksOf(c);
            for (MachineCatalog.Machine m : c.machines()) {
                machines.putIfAbsent(m.smType(), m);
                if (locks.containsKey(m.smType())) {
                    entityLocks.putIfAbsent(m.smType(), locks.get(m.smType()));
                }
            }
        }
        for (EventCatalog c : EventCatalogs.all()) {
            c.events().forEach(t -> eventTypes.putIfAbsent(t.name(), t));
        }
        this.samples = loadSamples();
    }

    /** {@code LOCKS} of a generated ecosystem catalog (smType → eventType → lock templates), if it has one. */
    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, List<String>>> locksOf(MachineCatalog c) {
        try {
            Object v = c.getClass().getField("LOCKS").get(null);
            return v instanceof Map<?, ?> m ? (Map<String, Map<String, List<String>>>) m : Map.of();
        } catch (ReflectiveOperationException | RuntimeException e) {
            return Map.of();
        }
    }

    private static List<Sample> loadSamples() {
        List<Sample> out = new ArrayList<>();
        String index = resource("concert-samples/index.txt");
        if (index == null) {
            return out;
        }
        for (String path : index.split("\n")) {
            if (path.isBlank()) {
                continue;
            }
            String text = resource("concert-samples/" + path.strip());
            if (text == null) {
                continue;
            }
            try {
                JsonNode n = Json.MAPPER.readTree(text);
                List<JsonNode> events = new ArrayList<>();
                n.path("events").forEach(events::add);
                String name = n.path("name").asText(path);
                boolean error = name.matches("(?i).*(poison|reject|out-of-stock|fail|error).*");
                out.add(new Sample(path.strip(), name, error, events));
            } catch (IOException e) {
                log.warn("bad sample flow {}: {}", path, e.getMessage());
            }
        }
        return out;
    }

    private static String resource(String path) {
        try (InputStream in = FlowGenerator.class.getClassLoader().getResourceAsStream(path)) {
            return in == null ? null : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }

    // ---------------------------------------------------------------- strategy per flow

    /** How events of {@code flowId} are made: {@code samples}, {@code trading}, {@code walk}, or null (cannot generate). */
    String strategy(String flowId) {
        if (flowId.startsWith("ev:")) {
            return samplesFor(flowId, false).isEmpty() ? null : "samples";
        }
        if (TRADING.contains(flowId)) {
            return "trading";
        }
        if (!samplesFor(flowId, false).isEmpty()) {
            return "samples";
        }
        return machines.containsKey(flowId) && !walk(machines.get(flowId).spec()).isEmpty() ? "walk" : null;
    }

    /** Sample flows of a flow: entity ones whose events target its smType; event ones with an event of its domain. */
    List<Sample> samplesFor(String flowId, boolean errors) {
        List<Sample> out = new ArrayList<>();
        for (Sample s : samples) {
            if (s.error() != errors) {
                continue;
            }
            boolean match = s.events().stream().anyMatch(e -> flowId.startsWith("ev:")
                    ? "event".equals(e.path("style").asText()) && eventTypes.containsKey(e.path("eventType").asText())
                            && eventTypes.get(e.path("eventType").asText()).domain().equals(flowId.substring(3))
                    : flowId.equals(e.path("smType").asText()));
            if (match) {
                out.add(s);
            }
        }
        return out;
    }

    /**
     * Every simple path (no state twice) from the initial state to a terminal one, longest first, at most 50; a
     * machine with no reachable terminal state gets one path of up to 5 steps along the first edges. Instances
     * rotate through them, so a flow sees each outcome (e.g. delivered, cancelled, refunded).
     */
    static List<List<StateMachineSpec.Edge>> paths(StateMachineSpec spec) {
        List<List<StateMachineSpec.Edge>> out = new ArrayList<>();
        dfs(spec, spec.initialState(), new ArrayList<>(), new LinkedHashSet<>(List.of(spec.initialState())), out);
        if (out.isEmpty()) {
            List<StateMachineSpec.Edge> path = new ArrayList<>();
            String s = spec.initialState();
            for (int i = 0; i < 5; i++) {
                String from = s;
                StateMachineSpec.Edge e = spec.edges().stream().filter(x -> x.from().equals(from)).findFirst().orElse(null);
                if (e == null) {
                    break;
                }
                path.add(e);
                s = e.to();
            }
            if (!path.isEmpty()) {
                out.add(path);
            }
        }
        out.sort(Comparator.comparingInt((List<StateMachineSpec.Edge> p) -> p.size()).reversed());
        return out;
    }

    private static void dfs(StateMachineSpec spec, String at, List<StateMachineSpec.Edge> path, Set<String> seen,
            List<List<StateMachineSpec.Edge>> out) {
        if (out.size() >= 50 || path.size() > 10) {
            return;
        }
        if (!path.isEmpty() && spec.isTerminal(at)) {
            out.add(List.copyOf(path));
            return;
        }
        for (StateMachineSpec.Edge e : spec.edges()) {
            if (e.from().equals(at) && seen.add(e.to())) {
                path.add(e);
                dfs(spec, e.to(), path, seen, out);
                path.removeLast();
                seen.remove(e.to());
            }
        }
    }

    /** The longest path to a terminal state (the first of {@link #paths}), or empty. */
    static List<StateMachineSpec.Edge> walk(StateMachineSpec spec) {
        List<List<StateMachineSpec.Edge>> ps = paths(spec);
        return ps.isEmpty() ? List.of() : ps.getFirst();
    }

    // ---------------------------------------------------------------- runs

    /** A running generator: instances started per second, for a duration, events queued by due time. */
    final class Run {
        final String flowId;
        final String strategy;
        final String runId = "gen" + Long.toString(System.currentTimeMillis() % 10_000_000, 36);
        final double rate;
        final int durationSec;
        final boolean errors;
        final long startedAt = System.currentTimeMillis();
        volatile boolean stopped;
        volatile long instances;
        volatile long sent;
        volatile long injected;
        volatile long failed;
        volatile String lastError;
        volatile long finishedAt;
        private final PriorityQueue<EventEnvelope> due = new PriorityQueue<>(Comparator.comparingLong(EventEnvelope::sourceTsMillis));
        private Iterator<EventEnvelope> trading;

        Run(String flowId, String strategy, double rate, int durationSec, boolean errors) {
            this.flowId = flowId;
            this.strategy = strategy;
            this.rate = rate;
            this.durationSec = durationSec;
            this.errors = errors;
        }

        boolean running() {
            return finishedAt == 0;
        }

        void loop() {
            double owed = 0;
            long last = System.currentTimeMillis();
            long end = startedAt + durationSec * 1000L;
            if (strategy.equals("trading")) {
                int orders = (int) Math.max(1, Math.ceil(rate * durationSec));
                TradingFlowGenerator.Config d = TradingFlowGenerator.Config.defaults(runId, startedAt);
                // About 8 events per order: pace the generator so orders start at the requested rate.
                trading = new TradingFlowGenerator(new TradingFlowGenerator.Config(d.seed(), d.volMultiplier(), runId, orders,
                        d.symbols(), d.accounts(), Math.max(5, (int) Math.ceil(rate * 4)), startedAt, Math.max(1, rate * 8),
                        errors ? 0.15 : d.rejectRate(), d.cancelRate(), d.limitRate()));
            }
            try {
                while (!stopped) {
                    long now = System.currentTimeMillis();
                    if (trading != null) {
                        while (trading.hasNext() && due.size() < 5000 && (due.isEmpty() || due.peek().sourceTsMillis() <= now + 1000)) {
                            EventEnvelope e = trading.next();
                            if (e.eventType().equals("submit")) {
                                instances++;
                            }
                            due.add(e);
                        }
                        if (!trading.hasNext() && due.isEmpty()) {
                            break;
                        }
                    } else if (now < end) {
                        owed += (now - last) * rate / 1000.0;
                        while (owed >= 1) {
                            owed--;
                            long n = ++instances;
                            boolean inject = errors && n % 5 == 0;
                            List<EventEnvelope> events = instance(n, inject, now);
                            if (inject && !events.isEmpty()) {
                                injected++;
                            }
                            due.addAll(events);
                        }
                    } else if (due.isEmpty()) {
                        break;
                    }
                    last = now;
                    List<EventEnvelope> batch = new ArrayList<>();
                    while (!due.isEmpty() && due.peek().sourceTsMillis() <= now && batch.size() < 500) {
                        batch.add(due.poll());
                    }
                    if (!batch.isEmpty()) {
                        publish(batch);
                    }
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                lastError = String.valueOf(e.getMessage());
                log.warn("generator for {} failed", flowId, e);
            } finally {
                finishedAt = System.currentTimeMillis();
            }
        }

        /** The events of instance {@code n}, due {@link #STEP_MS} apart from {@code now}. */
        List<EventEnvelope> instance(long n, boolean inject, long now) {
            String sfx = runId + "-" + n;
            List<EventEnvelope> out = new ArrayList<>();
            LongFunction<Long> at = i -> now + i * STEP_MS;
            if (strategy.equals("walk")) {
                MachineCatalog.Machine m = machines.get(flowId);
                String key = flowId.toUpperCase().replace('_', '-') + "-" + sfx;
                List<List<StateMachineSpec.Edge>> all = paths(m.spec());
                List<StateMachineSpec.Edge> path = all.isEmpty() ? List.of() : all.get((int) (n % all.size()));
                if (inject) {
                    // An event the initial state does not accept: rejected by the machine.
                    String initial = m.spec().initialState();
                    m.spec().edges().stream().filter(e -> !e.from().equals(initial)
                                    && m.spec().edge(initial, e.eventType()).isEmpty()).findFirst()
                            .ifPresent(e -> out.add(entity(flowId, key, e.eventType(), null, at.apply(0))));
                    return out;
                }
                for (int i = 0; i < path.size(); i++) {
                    out.add(entity(flowId, key, path.get(i).eventType(), null, at.apply(i)));
                }
                return out;
            }
            List<Sample> pool = samplesFor(flowId, inject);
            boolean invalidOrder = false;
            if (pool.isEmpty()) {
                pool = samplesFor(flowId, false);
                invalidOrder = inject; // no error samples: send the happy path's last event first (invalid there)
            }
            if (pool.isEmpty()) {
                return out;
            }
            Sample s = pool.get((int) (n % pool.size()));
            List<JsonNode> events = rewrite(s, sfx);
            if (invalidOrder && flowId.startsWith("ev:")) {
                return out; // event types have no state to violate; skip the injection
            }
            if (invalidOrder) {
                JsonNode e = events.getLast();
                out.add(fromSample(e, at.apply(0)));
                return out;
            }
            for (int i = 0; i < events.size(); i++) {
                out.add(fromSample(events.get(i), at.apply(i)));
            }
            return out;
        }

        private void publish(List<EventEnvelope> batch) {
            List<PutRecordsRequestEntry> entries = new ArrayList<>(batch.size());
            for (EventEnvelope e : batch) {
                EventEnvelope stamped = restamp(e, System.currentTimeMillis());
                entries.add(PutRecordsRequestEntry.builder().partitionKey(stamped.effectiveLockKeys().getFirst())
                        .data(SdkBytes.fromUtf8String(Json.write(stamped))).build());
            }
            try {
                String stream = streamOf.apply(flowId);
                PutRecordsResponse r = kinesis.putRecords(b -> b.streamName(stream).records(entries));
                int bad = r.failedRecordCount() == null ? 0 : r.failedRecordCount();
                sent += entries.size() - bad;
                failed += bad;
                if (bad > 0) {
                    lastError = bad + " records throttled or failed in the last batch";
                }
            } catch (RuntimeException ex) {
                failed += entries.size();
                lastError = "Kinesis put failed: " + ex.getMessage();
            }
        }

        Map<String, Object> status() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("flow", flowId);
            m.put("strategy", strategy);
            m.put("runId", runId);
            m.put("running", running());
            m.put("rate", rate);
            m.put("durationSec", durationSec);
            m.put("errors", errors);
            m.put("startedAt", startedAt);
            m.put("finishedAt", finishedAt == 0 ? null : finishedAt);
            m.put("elapsedSec", ((finishedAt == 0 ? System.currentTimeMillis() : finishedAt) - startedAt) / 1000);
            m.put("instances", instances);
            m.put("sent", sent);
            m.put("injected", injected);
            m.put("failed", failed);
            m.put("lastError", lastError);
            return m;
        }
    }

    /** Sample events with each identity value (entity instance keys, event lock-key values) suffixed. */
    List<JsonNode> rewrite(Sample s, String sfx) {
        Set<String> ids = new LinkedHashSet<>();
        for (JsonNode e : s.events()) {
            if (e.hasNonNull("instanceKey")) {
                ids.add(e.path("instanceKey").asText());
            }
            // entity events: the payload fields their lock keys are rendered from (e.g. an execution's orderId) are ids
            // too; left as in the sample, every generated instance would queue on the same lock
            Map<String, List<String>> locks = entityLocks.get(e.path("smType").asText());
            if (!"event".equals(e.path("style").asText()) && locks != null) {
                for (String tpl : locks.getOrDefault(e.path("eventType").asText(), List.of())) {
                    for (String field : LockTemplates.placeholders(tpl)) {
                        JsonNode v = e.path("payload").path(field);
                        if (v.isTextual()) {
                            ids.add(v.asText());
                        }
                    }
                }
            }
            EventCatalog.EventType t = eventTypes.get(e.path("eventType").asText());
            if ("event".equals(e.path("style").asText()) && t != null) {
                for (String tpl : t.lockTemplates()) {
                    for (String field : LockTemplates.placeholders(tpl)) {
                        JsonNode v = e.path("payload").path(field);
                        if (v.isTextual()) {
                            ids.add(v.asText());
                        }
                    }
                }
            }
        }
        String text = Json.write(s.events());
        // Longest first, so an id that prefixes another (ORDER-1 / ORDER-10) can't half-match.
        List<String> sorted = new ArrayList<>(ids);
        sorted.sort(Comparator.comparingInt(String::length).reversed());
        for (String id : sorted) {
            text = text.replace("\"" + id + "\"", "\"" + id + "-" + sfx + "\"");
        }
        try {
            List<JsonNode> out = new ArrayList<>();
            Json.MAPPER.readTree(text).forEach(out::add);
            return out;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private EventEnvelope fromSample(JsonNode e, long dueAt) {
        String payload = e.path("payload").isMissingNode() || e.path("payload").isNull() ? null : e.path("payload").toString();
        if ("event".equals(e.path("style").asText())) {
            String type = e.path("eventType").asText();
            EventCatalog.EventType t = eventTypes.get(type);
            String id = e.hasNonNull("id") ? e.path("id").asText() + "-" + UUID.randomUUID().toString().substring(0, 8)
                    : type + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            String json = bind(payload, t == null ? null : t.payloadType());
            List<String> locks = t == null || t.lockTemplates().isEmpty() ? List.of()
                    : LockTemplates.render(t.lockTemplates(), id, tree(json));
            EventEnvelope env = EventEnvelope.event(id, t == null ? "unknown" : t.domain(), type, locks, json, dueAt);
            long scheduled = scheduledAt(e.path("at").asText(null));
            return scheduled > 0 ? env.withScheduledAt(scheduled) : env;
        }
        return entity(e.path("smType").asText(), e.path("instanceKey").asText(), e.path("eventType").asText(), payload, dueAt);
    }

    private EventEnvelope entity(String smType, String key, String eventType, String payload, long dueAt) {
        MachineCatalog.Machine m = machines.get(smType);
        Class<?> type = m == null ? null : m.spec().edges().stream().filter(x -> x.eventType().equals(eventType))
                .map(StateMachineSpec.Edge::payloadType).filter(java.util.Objects::nonNull).findFirst().orElse(null);
        String json = bind(payload, type);
        List<String> templates = entityLocks.getOrDefault(smType, Map.of()).get(eventType);
        List<String> locks = templates == null || templates.isEmpty() ? List.of() : renderOrEmpty(templates, key, json);
        return new EventEnvelope(smType + "-" + eventType + "-" + UUID.randomUUID(), smType, key, eventType, locks, json, dueAt, 0);
    }

    private static List<String> renderOrEmpty(List<String> templates, String key, String json) {
        try {
            return LockTemplates.render(templates, key, tree(json));
        } catch (RuntimeException e) {
            return List.of(); // a template field the payload lacks: lock the entity key only
        }
    }

    /** The envelope with its producer timestamp set to the moment it is published. */
    static EventEnvelope restamp(EventEnvelope e, long ts) {
        return new EventEnvelope(e.eventId(), e.smType(), e.instanceKey(), e.eventType(), e.lockKeys(), e.payload(), ts,
                e.ingestTsMillis(), e.style(), e.scheduledAtMillis(), e.parentEventId(), e.causationRoot());
    }

    /** The payload bound to its model class and written back (as the ecosystem senders do), else as given. */
    private static String bind(String payload, Class<?> type) {
        if (payload == null || type == null) {
            return payload;
        }
        try {
            return ModelJson.write(ModelJson.read(payload, type));
        } catch (RuntimeException e) {
            return payload;
        }
    }

    private static JsonNode tree(String json) {
        try {
            return json == null ? Json.MAPPER.createObjectNode() : Json.MAPPER.readTree(json);
        } catch (IOException e) {
            ObjectNode empty = Json.MAPPER.createObjectNode();
            return empty;
        }
    }

    /** {@code +30s / +5m / +2h} from now, or an ISO instant; 0 when absent. */
    static long scheduledAt(String at) {
        if (at == null || at.isBlank()) {
            return 0;
        }
        if (at.startsWith("+")) {
            long n = Long.parseLong(at.substring(1, at.length() - 1));
            long unit = switch (at.charAt(at.length() - 1)) {
                case 's' -> 1000L;
                case 'm' -> 60_000L;
                case 'h' -> 3_600_000L;
                case 'd' -> 86_400_000L;
                default -> throw new IllegalArgumentException("bad at " + at);
            };
            return System.currentTimeMillis() + n * unit;
        }
        return Instant.parse(at).toEpochMilli();
    }

    // ---------------------------------------------------------------- API

    synchronized Map<String, Object> start(String flowId, double rate, int durationSec, boolean errors) {
        Run current = runs.get(flowId);
        if (current != null && current.running()) {
            throw new IllegalStateException("a generator is already running for " + flowId);
        }
        String strategy = strategy(flowId);
        if (strategy == null) {
            throw new IllegalArgumentException("no way to generate events for " + flowId
                    + ": no sample flows and no machine spec to walk");
        }
        Run run = new Run(flowId, strategy, Math.max(0.1, Math.min(rate, 200)), Math.max(5, Math.min(durationSec, 3600)), errors);
        runs.put(flowId, run);
        Thread.ofVirtual().name("generate-" + flowId).start(run::loop);
        return run.status();
    }

    Map<String, Object> stop(String flowId) {
        Run run = runs.get(flowId);
        if (run != null) {
            run.stopped = true;
        }
        return status(flowId);
    }

    Map<String, Object> status(String flowId) {
        Run run = runs.get(flowId);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("available", strategy(flowId));
        out.put("samples", (flowId.startsWith("ev:") || !TRADING.contains(flowId))
                ? samplesFor(flowId, false).stream().map(Sample::name).toList() : List.of());
        out.put("errorSamples", samplesFor(flowId, true).stream().map(Sample::name).toList());
        out.put("run", run == null ? null : run.status());
        return out;
    }

    @Override
    public void close() {
        runs.values().forEach(r -> r.stopped = true);
    }
}
