package io.concert.ecosystem;

import static io.concert.ecosystem.JavaSources.GENERATED_HEADER;
import static io.concert.ecosystem.JavaSources.html;
import static io.concert.ecosystem.JavaSources.q;

import io.concert.model.codegen.JavaNames;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.PropertyDef;
import io.concert.model.pure.ResolvedModel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Java sources of an ecosystem's event-style part ({@code concert::event}): one {@code EventCatalog} per domain,
 * the {@code <Name>EventTypes} registry (catalogs, handlers, diagrams), the event worker main, the user-owned handler
 * stubs and the generated flow test.
 */
final class EventSources {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([^}]*)}");

    private final Ecosystem eco;
    private final ResolvedModel model;

    EventSources(Ecosystem eco) {
        this.eco = eco;
        this.model = eco.model();
    }

    String pkg() {
        return eco.javaPackage();
    }

    static String catalogClass(String domain) {
        return Ecosystem.pascal(domain) + "EventCatalog";
    }

    String typesClass() {
        return eco.pascalName() + "EventTypes";
    }

    String workerMainClass() {
        return eco.pascalName() + "EventWorkerMain";
    }

    String testClass() {
        return eco.pascalName() + "EventFlowsTest";
    }

    /** {@code OrderCreateEvent} -> {@code ORDER_CREATE_EVENT}. */
    static String constant(String name) {
        return EventDecl.snake(name).toUpperCase(Locale.ROOT);
    }

    // ------------------------------------------------------------------ flow diagram

    /** Mermaid flowchart: one subgraph per domain, an edge per emits entry (scheduled ones are not distinguished). */
    String flowMermaid() {
        StringBuilder sb = new StringBuilder("flowchart LR\n");
        for (String d : eco.domains()) {
            sb.append("  subgraph ").append(d).append("[\"domain ").append(d).append("\"]\n");
            for (EventDecl e : eco.eventsOf(d)) {
                String keys = e.locks().isEmpty() ? d + ":{id}" : String.join(", ", e.locks());
                sb.append("    ").append(e.name()).append("[\"").append(e.name()).append("<br/><small>")
                        .append(keys.replace("\"", "'")).append(e.onError().equals("NON_BLOCKING") ? " · non-blocking" : "")
                        .append("</small>\"]\n");
            }
            sb.append("  end\n");
        }
        for (EventDecl e : eco.events()) {
            for (String t : e.emits()) {
                sb.append("  ").append(e.name()).append(" --> ").append(t).append('\n');
            }
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------ catalogs

    /** Domains whose events lock a key of the state's shape (else the first domain). */
    List<String> domainsOf(EventDecl.StateDecl s) {
        List<String> out = new ArrayList<>();
        if (s.keyTemplate() != null) {
            String shape = shape(s.keyTemplate());
            for (EventDecl e : eco.events()) {
                if (e.locks().stream().anyMatch(l -> shape(l).equals(shape)) && !out.contains(e.domain())) {
                    out.add(e.domain());
                }
            }
        }
        if (out.isEmpty()) {
            out.add(eco.domains().getFirst());
        }
        return out;
    }

    private static String shape(String template) {
        return PLACEHOLDER.matcher(template).replaceAll("{}");
    }

    String catalog(String domain) {
        String cls = catalogClass(domain);
        List<EventDecl> events = eco.eventsOf(domain);
        List<EventDecl.StateDecl> states = eco.states().stream().filter(s -> domainsOf(s).contains(domain)).toList();
        StringBuilder sb = new StringBuilder(GENERATED_HEADER);
        sb.append("package ").append(pkg()).append(";\n\n");
        sb.append("import io.concert.sdk.events.EventCatalog;\nimport io.concert.sdk.events.OnError;\nimport java.util.List;\n"
                + "import java.util.Map;\n\n");
        sb.append("/**\n * Event types of domain {@code ").append(domain).append("} (task queue {@code ev-").append(domain)
                .append("}), declared with {@code concert::event} in the\n * Pure models; discovered through the {@link EventCatalog} SPI "
                        + "(workers, event sender, trace UI).\n *\n * <pre>\n");
        for (EventDecl e : events) {
            sb.append(" *   ").append(html(e.name())).append("  locks ").append(html(e.locks().isEmpty() ? domain + ":{id}" : String.join(", ", e.locks())))
                    .append("  onError ").append(e.onError()).append("  attempts ").append(e.maxAttempts());
            if (!e.emits().isEmpty()) {
                sb.append("  emits ").append(html(String.join(", ", e.emits())));
            }
            sb.append('\n');
        }
        sb.append(" * </pre>\n */\n");
        sb.append("public final class ").append(cls).append(" implements EventCatalog {\n\n");
        sb.append("    public static final String DOMAIN = ").append(q(domain)).append(";\n\n");
        for (EventDecl e : events) {
            sb.append("    /** {@code ").append(html(e.cls().qualifiedName())).append("} (").append(html(e.cls().location().toString()))
                    .append("). */\n");
            sb.append("    public static final EventType ").append(constant(e.name())).append(" = new EventType(").append(q(e.name()))
                    .append(", DOMAIN,\n            ").append(eco.javaType(e.cls().qualifiedName())).append(".class, List.of(")
                    .append(String.join(", ", e.locks().stream().map(JavaSources::q).toList())).append("), OnError.").append(e.onError())
                    .append(", ").append(e.maxAttempts()).append(");\n\n");
        }
        for (EventDecl.StateDecl s : states) {
            sb.append("    /** Keyed state {@code ").append(html(s.cls().qualifiedName())).append("}: rows {@code state:<key>}. */\n");
            sb.append("    public static final StateType ").append(constant(s.name())).append("_STATE = new StateType(").append(q(s.name()))
                    .append(", ").append(eco.javaType(s.cls().qualifiedName())).append(".class, ")
                    .append(s.keyTemplate() == null ? "null" : q(s.keyTemplate())).append(");\n\n");
        }
        sb.append("    public static final List<EventType> EVENTS = List.of(")
                .append(String.join(", ", events.stream().map(e -> constant(e.name())).toList())).append(");\n\n");
        sb.append("    public static final List<StateType> STATES = List.of(")
                .append(String.join(", ", states.stream().map(s -> constant(s.name()) + "_STATE").toList())).append(");\n\n");
        sb.append("    /** {@code concert::event.emits} per event type. */\n");
        sb.append("    public static final Map<String, List<String>> EMITS = Map.ofEntries(");
        List<String> entries = new ArrayList<>();
        for (EventDecl e : events) {
            entries.add("\n            Map.entry(" + q(e.name()) + ", List.of(" + String.join(", ", e.emits().stream().map(JavaSources::q).toList())
                    + "))");
        }
        sb.append(String.join(",", entries)).append(");\n\n");
        sb.append("    @Override\n    public String name() {\n        return ").append(q(eco.name() + "/" + domain)).append(";\n    }\n\n");
        sb.append("    @Override\n    public List<EventType> events() {\n        return EVENTS;\n    }\n\n");
        sb.append("    @Override\n    public List<StateType> states() {\n        return STATES;\n    }\n\n");
        sb.append("    @Override\n    public Map<String, List<String>> emits() {\n        return EMITS;\n    }\n\n");
        sb.append("    @Override\n    public String flowMermaid() {\n        return ").append(typesClass()).append(".FLOW_MERMAID;\n    }\n\n");
        sb.append("    @Override\n    public String modelMermaid() {\n        return ").append(typesClass()).append(".MODEL_MERMAID;\n    }\n");
        sb.append("}\n");
        return sb.toString();
    }

    // ------------------------------------------------------------------ <Name>EventTypes

    String types() {
        String cls = typesClass();
        StringBuilder sb = new StringBuilder(GENERATED_HEADER);
        sb.append("package ").append(pkg()).append(";\n\n");
        sb.append("import io.concert.sdk.events.EventCatalog;\nimport io.concert.sdk.events.EventCatalogs;\n"
                + "import io.concert.sdk.events.HandlerRegistry;\nimport java.util.List;\n\n");
        sb.append("/**\n * The event-style part of ecosystem {@code ").append(eco.name()).append("}: its catalogs (one per domain), the handlers\n"
                + " * of each domain (your {@code *Handler} classes) and the diagrams.\n *\n * <pre>\n");
        flowMermaid().lines().forEach(l -> sb.append(" * ").append(html(l)).append('\n'));
        sb.append(" * </pre>\n */\n");
        sb.append("public final class ").append(cls).append(" {\n    private ").append(cls).append("() {}\n\n");
        sb.append("    /** The ecosystem (module) name. */\n    public static final String NAME = ").append(q(eco.name())).append(";\n\n");
        sb.append("    /** Mermaid flowchart of the event flow (concert::event.emits). */\n");
        sb.append("    public static final String FLOW_MERMAID = ").append(q(flowMermaid())).append(";\n\n");
        sb.append("    /** Mermaid class diagram of the ecosystem's model (all model files). */\n");
        sb.append("    public static final String MODEL_MERMAID = mermaid(")
                .append(String.join(", ", eco.modelRegistryClasses().stream().map(r -> r + ".MERMAID").toList())).append(");\n\n");
        sb.append("    /** The domains, in declaration order. */\n    public static final List<String> DOMAINS = List.of(")
                .append(String.join(", ", eco.domains().stream().map(JavaSources::q).toList())).append(");\n\n");
        sb.append("    /** One catalog per domain. */\n    public static final List<EventCatalog> CATALOGS = List.of(")
                .append(String.join(", ", eco.domains().stream().map(d -> "new " + catalogClass(d) + "()").toList())).append(");\n\n");
        sb.append("    /** Registers the catalogs (also discovered through ServiceLoader; registering twice is harmless). */\n");
        sb.append("    public static void register() {\n        CATALOGS.forEach(EventCatalogs::register);\n    }\n\n");
        sb.append("    /** The handlers of a domain: one instance of each of your handler classes. */\n");
        sb.append("    public static HandlerRegistry handlers(String domain) {\n        register();\n        return switch (domain) {\n");
        for (String d : eco.domains()) {
            sb.append("            case ").append(q(d)).append(" -> new HandlerRegistry(").append(q(d)).append(")");
            for (EventDecl e : eco.eventsOf(d)) {
                sb.append("\n                    .add(new ").append(e.handlerClass()).append("())");
            }
            sb.append(";\n");
        }
        sb.append("            default -> throw new IllegalArgumentException(\"unknown event domain \" + domain + \", expected one of \" + DOMAINS);\n");
        sb.append("        };\n    }\n\n");
        sb.append("""
                    /** The event type named {@code eventType}. */
                    public static EventCatalog.EventType type(String eventType) {
                        for (EventCatalog c : CATALOGS) {
                            for (EventCatalog.EventType t : c.events()) {
                                if (t.name().equals(eventType)) {
                                    return t;
                                }
                            }
                        }
                        throw new IllegalArgumentException("unknown event type " + eventType + ", expected one of " + typeNames());
                    }

                    /** Whether {@code name} is one of the event types. */
                    public static boolean isEventType(String name) {
                        return typeNames().contains(name);
                    }

                    /** All event type names, in declaration order. */
                    public static List<String> typeNames() {
                        return CATALOGS.stream().flatMap(c -> c.events().stream()).map(EventCatalog.EventType::name).toList();
                    }

                    /** Every event type, in declaration order. */
                    public static List<EventCatalog.EventType> types() {
                        return CATALOGS.stream().flatMap(c -> c.events().stream()).toList();
                    }

                    /** The {@code concert::event.emits} of an event type. */
                    public static List<String> emits(String eventType) {
                        for (EventCatalog c : CATALOGS) {
                            List<String> e = c.emits().get(eventType);
                            if (e != null) {
                                return e;
                            }
                        }
                        return List.of();
                    }

                    /** Merges generated {@code classDiagram}s (one per model file) into one diagram. */
                    static String mermaid(String... diagrams) {
                        StringBuilder sb = new StringBuilder("classDiagram\\n");
                        for (String d : diagrams) {
                            sb.append(d.substring(d.indexOf('\\n') + 1));
                        }
                        return sb.toString();
                    }
                }
                """);
        return sb.toString();
    }

    // ------------------------------------------------------------------ worker main

    String workerMain() {
        String cls = workerMainClass();
        String t = typesClass();
        String env = eco.name().toUpperCase(Locale.ROOT).replace('-', '_') + "_EVENT_DOMAINS";
        return GENERATED_HEADER + "package " + pkg() + ";\n\n"
                + """
                import io.concert.common.Env;
                import io.concert.common.TemporalClients;
                import io.concert.common.WorkerTuning;
                import io.concert.sdk.EntitySnapshotPublisher;
                import io.concert.sdk.events.EventWorkerBootstrap;
                import io.concert.sink.SnapshotCodec;
                import io.concert.store.StateStore;
                import io.concert.store.Stores;
                import io.temporal.worker.WorkerFactory;
                import org.slf4j.Logger;
                import org.slf4j.LoggerFactory;

                """
                + "/**\n * Runs the event-style workers of ecosystem {@code " + eco.name() + "} in one process: one worker per domain (task\n"
                + " * queue {@code ev-<domain>}) with its processor workflow and your handlers. Environment as for every worker\n"
                + " * ({@code TEMPORAL_*}, {@code STORE_KIND}, {@code KAFKA_BOOTSTRAP}, ...); {@code " + env + "} (comma separated)\n"
                + " * optionally restricts the domains.\n */\n"
                + "public final class " + cls + " {\n"
                + "    private static final Logger log = LoggerFactory.getLogger(" + cls + ".class);\n\n"
                + "    private " + cls + "() {}\n\n"
                + "    public static void main(String[] args) throws InterruptedException {\n"
                + "        StateStore store = Stores.fromEnv();\n"
                + "        var client = TemporalClients.fromEnv();\n"
                + "        " + t + ".register();\n"
                + "        // Lifecycle rows (evt_*) and state documents (st_*) go to entity-snapshots like completed entities (no-op\n"
                + "        // when KAFKA_BOOTSTRAP is empty); the schemaHash header is the hash of the ecosystem's model diagram.\n"
                + "        String schemaHash = SnapshotCodec.schemaHash(" + t + ".MODEL_MERMAID);\n"
                + "        EntitySnapshotPublisher snapshots = EntitySnapshotPublisher.fromEnv(\n"
                + "                type -> type.startsWith(\"evt_\") || type.startsWith(\"st_\") ? schemaHash : null);\n"
                + "        String domains = Env.get(\"" + env + "\", String.join(\",\", " + t + ".DOMAINS));\n"
                + "        WorkerFactory factory = WorkerFactory.newInstance(client, WorkerTuning.factoryOptions());\n"
                + "        for (String domain : domains.split(\",\")) {\n"
                + "            if (domain.isBlank()) {\n"
                + "                continue;\n"
                + "            }\n"
                + "            EventWorkerBootstrap.register(factory, client, store, " + t + ".handlers(domain.trim()),\n"
                + "                    EventWorkerBootstrap.Options.defaults().withPublisher(snapshots));\n"
                + "            log.info(\"event worker up for ev-{}\", domain.trim());\n"
                + "        }\n"
                + "        factory.start();\n"
                + "        Runtime.getRuntime().addShutdownHook(new Thread(() -> {\n"
                + "            factory.shutdown();\n"
                + "            store.close();\n"
                + "        }));\n"
                + "        Thread.currentThread().join();\n"
                + "    }\n"
                + "}\n";
    }

    // ------------------------------------------------------------------ handler stubs

    /** States whose key a handler of {@code e} can render from the event and lock: (state, Java key expression). */
    private Map<EventDecl.StateDecl, String> loadableStates(EventDecl e, String var) {
        Map<EventDecl.StateDecl, String> out = new LinkedHashMap<>();
        Set<String> lockShapes = new LinkedHashSet<>();
        e.locks().forEach(l -> lockShapes.add(shape(l)));
        for (EventDecl.StateDecl s : eco.states()) {
            if (s.keyTemplate() == null || !lockShapes.contains(shape(s.keyTemplate()))) {
                continue;
            }
            String expr = keyExpression(s.keyTemplate(), e, var);
            if (expr != null) {
                out.put(s, expr);
            }
        }
        return out;
    }

    /** {@code order:{orderId}} -> {@code "order:" + event.getOrderId()}, or null if a field is not on the event. */
    private String keyExpression(String template, EventDecl e, String var) {
        List<String> parts = new ArrayList<>();
        Matcher m = PLACEHOLDER.matcher(template);
        int last = 0;
        while (m.find()) {
            if (m.start() > last) {
                parts.add(q(template.substring(last, m.start())));
            }
            String field = m.group(1).trim();
            Optional<PropertyDef> p = property(e.cls(), field);
            if (p.isEmpty()) {
                return null;
            }
            parts.add(var + "." + JavaNames.getter(field) + "()");
            last = m.end();
        }
        if (last < template.length()) {
            parts.add(q(template.substring(last)));
        }
        return String.join(" + ", parts);
    }

    private Optional<PropertyDef> property(ClassDef c, String name) {
        return model.allProperties(c).stream().filter(p -> p.name().equals(name)).findFirst();
    }

    /** Setter calls copying the properties {@code target} shares (name, type, multiplicity) with the event. */
    private String copies(EventDecl e, ClassDef target, String var) {
        StringBuilder sb = new StringBuilder();
        for (PropertyDef p : model.allProperties(target)) {
            Optional<PropertyDef> src = property(e.cls(), p.name());
            if (src.isPresent() && src.get().type().equals(p.type()) && src.get().multiplicity().equals(p.multiplicity())) {
                sb.append('.').append(JavaNames.setter(p.name())).append('(').append(var).append('.').append(JavaNames.getter(p.name()))
                        .append("())");
            }
        }
        return sb.toString();
    }

    String stub(EventDecl e) {
        String cls = e.handlerClass();
        String eventType = eco.javaType(e.cls().qualifiedName());
        String eventSimple = Ecosystem.simpleName(eventType);
        String var = "event";
        TreeSet<String> imports = new TreeSet<>(List.of("io.concert.sdk.events.EventContext", "io.concert.sdk.events.EventHandler",
                "io.concert.sdk.events.Handles", eventType));
        Map<String, String> names = new LinkedHashMap<>();
        names.put(eventType, eventSimple);
        names.put("Handles", "Handles");
        java.util.function.Function<String, String> use = qualified -> names.computeIfAbsent(qualified, k -> {
            String simple = Ecosystem.simpleName(k);
            if (names.containsValue(simple) || simple.equals(cls)) {
                return k;
            }
            imports.add(k);
            return simple;
        });

        StringBuilder body = new StringBuilder();
        body.append("    @Override\n    public void apply(").append(eventSimple).append(' ').append(var).append(", EventContext ctx) {\n");
        body.append("        // TODO ").append(e.name()).append(": validate the event, load / change / save keyed state and emit follow-up events.\n");
        Map<EventDecl.StateDecl, String> states = loadableStates(e, var);
        if (!states.isEmpty()) {
            body.append("        //\n        // Keyed state: load it under one of this event's lock keys, change it, save it (written once the handler\n"
                    + "        // returns, version-guarded and exactly once per event):\n");
            for (Map.Entry<EventDecl.StateDecl, String> s : states.entrySet()) {
                String st = use.apply(eco.javaType(s.getKey().cls().qualifiedName()));
                String sv = Character.toLowerCase(Ecosystem.simpleName(st).charAt(0)) + Ecosystem.simpleName(st).substring(1);
                if (sv.equals(var) || sv.equals("ctx")) {
                    sv = sv + "State";
                }
                body.append("        // ").append(st).append(' ').append(sv).append(" = ctx.state(").append(st).append(".class, ")
                        .append(s.getValue()).append(").orElseGet(").append(st).append("::new);\n");
                body.append("        // ctx.save(").append(sv).append(");\n");
            }
        } else if (!eco.states().isEmpty()) {
            body.append("        //\n        // Keyed state: ctx.state(Type.class, key) / ctx.save(state); the key must be one of this event's lock keys\n"
                    + "        // ").append(e.locks().isEmpty() ? "(" + e.domain() + ":<eventId> by default)" : e.locks()).append(".\n");
        }
        List<EventDecl> targets = e.emits().stream().map(n -> eco.event(n).orElseThrow()).toList();
        body.append("        //\n        // Follow-up events (their domain and lock keys come from their concert::event annotations; ids are\n"
                + "        // ").append("<eventId>.<n>, enqueued exactly once after this handler returns):\n");
        if (targets.isEmpty()) {
            body.append("        // ctx.emit(new SomeEvent()...);\n");
            body.append("        // ctx.emitAt(Instant.now().plus(Duration.ofMinutes(2)), new SomeEvent()...);   // SCHEDULED until then\n");
        }
        for (int i = 0; i < targets.size(); i++) {
            EventDecl t = targets.get(i);
            String tt = use.apply(eco.javaType(t.cls().qualifiedName()));
            String call = "new " + tt + "()" + copies(e, t.cls(), var);
            if (i == targets.size() - 1 && targets.size() > 0) {
                imports.add("java.time.Duration");
                imports.add("java.time.Instant");
                body.append("        // ctx.emit(").append(call).append(");\n");
                body.append("        // ctx.emitAt(Instant.now().plus(Duration.ofMinutes(2)), ").append(call).append(");   // SCHEDULED until then\n");
            } else {
                body.append("        // ctx.emit(").append(call).append(");\n");
            }
        }
        if (targets.isEmpty()) {
            imports.add("java.time.Duration");
            imports.add("java.time.Instant");
        }
        imports.add("io.concert.sdk.events.BlockingError");
        imports.add("io.concert.sdk.events.NonBlockingError");
        body.append("        //\n        // Errors: throw new NonBlockingError(\"...\") parks the event (ERROR_NON_BLOCKING, keys released, operator\n"
                + "        // retry / skip); throw new BlockingError(\"...\") keeps its keys held (ERROR_BLOCKING: later events on them\n"
                + "        // wait) until an operator retries or skips it; any other exception is retried (").append(e.maxAttempts())
                .append(" attempts), then\n        // handled as ").append(e.onError()).append(" (concert::event.onError).\n");
        body.append("        // if (...) {\n        //     throw new NonBlockingError(\"").append(e.name()).append(" cannot be applied yet\");\n        // }\n");
        body.append("    }\n}\n");

        StringBuilder sb = new StringBuilder();
        sb.append("// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force\n"
                + "// regenerates it after backing it up as ").append(cls).append(".java.bak-<timestamp>).\n");
        sb.append("package ").append(pkg()).append(";\n\n");
        imports.forEach(i -> sb.append("import ").append(i).append(";\n"));
        sb.append("\n/**\n * Handles {@code ").append(html(e.name())).append("} ({@code ").append(html(e.cls().qualifiedName())).append("}), domain {@code ")
                .append(e.domain()).append("}: lock keys {@code ")
                .append(html(e.locks().isEmpty() ? e.domain() + ":<eventId>" : String.join(", ", e.locks()))).append("}, on error ")
                .append(e.onError()).append(" after\n * ").append(e.maxAttempts()).append(" attempts");
        if (!e.emits().isEmpty()) {
            sb.append(", emits ").append(html(String.join(", ", e.emits())));
        }
        sb.append(".\n *\n * <p>It runs in a local activity of the event's processor with all of the event's lock keys held, so it may do\n"
                + " * I/O; it may run more than once (retries), but its {@code ctx.save} / {@code ctx.emit} calls are applied once, after\n"
                + " * it returns. Returning normally makes the event DONE.\n */\n");
        sb.append("@Handles(").append(eventSimple).append(".class)\n");
        sb.append("public class ").append(cls).append(" implements EventHandler<").append(eventSimple).append("> {\n\n");
        sb.append(body);
        return sb.toString();
    }

    // ------------------------------------------------------------------ generated test

    String test(String eventsSender) {
        String cls = testClass();
        String t = typesClass();
        return GENERATED_HEADER + "package " + pkg() + ";\n\n"
                + """
                import static org.junit.jupiter.api.Assertions.assertEquals;
                import static org.junit.jupiter.api.Assertions.assertNotNull;
                import static org.junit.jupiter.api.Assertions.assertTrue;

                import com.fasterxml.jackson.databind.JsonNode;
                import io.concert.common.EventEnvelope;
                import io.concert.common.Json;
                import io.concert.common.TaskQueues;
                import io.concert.common.api.SmStateRow;
                import io.concert.model.runtime.ModelJson;
                import io.concert.model.runtime.ModelObject;
                import io.concert.orchestration.DispatchActivitiesImpl;
                import io.concert.orchestration.IngestDispatcher;
                import io.concert.orchestration.KeyLockWorkflowImpl;
                import io.concert.sdk.EntitySnapshotPublisher;
                import io.concert.sdk.events.EventCatalog;
                import io.concert.sdk.events.EventWorkerBootstrap;
                import io.concert.store.InMemoryStateStore;
                import io.concert.store.TraceWriter;
                import io.temporal.testing.TestEnvironmentOptions;
                import io.temporal.testing.TestWorkflowEnvironment;
                import io.temporal.worker.Worker;
                import java.io.IOException;
                import java.nio.file.Files;
                import java.nio.file.Path;
                import java.util.ArrayDeque;
                import java.util.ArrayList;
                import java.util.Deque;
                import java.util.List;
                import java.util.Map;
                import java.util.Set;
                import java.util.stream.Stream;
                import org.junit.jupiter.api.Test;

                """
                + "/**\n * Checks the generated event samples and runs every event flow ({@code samples/event-flows/*.json}) through the real\n"
                + " * pipeline on Temporal's in-memory server: ingest dispatcher, lock chain, processors and your handlers. Each event\n"
                + " * of a flow is sent once the previous one's causation tree settled (no event NEW any more). A flow may list the\n"
                + " * expected status per event type ({@code \"expect\": {\"OrderCreateEvent\": \"DONE\"}}); without it, no event may\n"
                + " * end without a handler.\n */\n"
                + "class " + cls + " {\n\n"
                + "    private static final Path SAMPLES = Path.of(System.getProperty(\"ecosystem.samples\", \"samples\"));\n\n"
                + "    @Test\n"
                + "    void eventSamplesBindAndEveryTypeHasAHandler() throws IOException {\n"
                + "        int checked = 0;\n"
                + "        for (EventCatalog.EventType type : " + t + ".types()) {\n"
                + "            assertNotNull(" + t + ".handlers(type.domain()).get(type.name()), \"no handler for \" + type.name());\n"
                + """
                            Path sample = SAMPLES.resolve("events").resolve(type.name() + ".json");
                            if (!Files.exists(sample)) {
                                continue;
                            }
                            Object bound = ModelJson.read(Files.readString(sample), type.payloadType());
                            if (bound instanceof ModelObject mo) {
                                assertEquals(List.of(), mo.validationErrors(), sample.toString());
                            }
                            checked++;
                        }
                        assertTrue(checked > 0, "no event samples under " + SAMPLES.toAbsolutePath());
                    }

                    @Test
                    void eventFlowsSettleThroughYourHandlers() throws IOException {
                        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "false");
                        InMemoryStateStore store = new InMemoryStateStore();
                        List<Path> flows;
                        try (Stream<Path> s = Files.list(SAMPLES.resolve("event-flows"))) {
                            flows = s.filter(p -> p.toString().endsWith(".json")).sorted().toList();
                        }
                        assertTrue(!flows.isEmpty(), "no event flows");
                        try (TestWorkflowEnvironment env = TestWorkflowEnvironment.newInstance(
                                TestEnvironmentOptions.newBuilder().setUseTimeskipping(false).build());
                                TraceWriter traces = new TraceWriter(store, 100_000, 500, 10)) {
                            DispatchActivitiesImpl dispatch = new DispatchActivitiesImpl(env.getWorkflowClient(), traces);
                            Worker orch = env.newWorker(TaskQueues.ORCHESTRATION);
                            orch.registerWorkflowImplementationTypes(KeyLockWorkflowImpl.class);
                            orch.registerActivitiesImplementations(dispatch);
                """
                + "            for (String domain : " + t + ".DOMAINS) {\n"
                + "                EventWorkerBootstrap.registerOn(env.newWorker(TaskQueues.events(domain)), env.getWorkflowClient(), store,\n"
                + "                        " + t + ".handlers(domain), EventWorkerBootstrap.Options.defaults().withPublisher(EntitySnapshotPublisher.logging()));\n"
                + "            }\n"
                + """
                            env.start();
                            try (IngestDispatcher dispatcher = new IngestDispatcher(store, traces, dispatch, Set.of(), 64)) {
                                int n = 0;
                                for (Path flow : flows) {
                                    JsonNode f = Json.MAPPER.readTree(Files.readString(flow));
                                    List<SmStateRow> rows = new ArrayList<>();
                                    for (JsonNode e : f.path("events")) {
                                        JsonNode p = e.get("payload");
                """
                + "                        EventEnvelope event = " + eventsSender + ".eventEnvelope(e.path(\"eventType\").asText(),\n"
                + """
                                                p == null || p.isNull() ? null : Json.write(p), 0, flow.getFileName() + "-" + n++);
                                        dispatcher.dispatchBatch(List.of(event));
                                        rows.addAll(settle(store, event.eventId(), flow.getFileName().toString()));
                                    }
                                    JsonNode expect = f.get("expect");
                                    for (SmStateRow r : rows) {
                                        JsonNode d = Json.MAPPER.readTree(r.data());
                                        String type = d.path("eventType").asText();
                                        if (expect != null && expect.has(type)) {
                                            assertEquals(expect.get(type).asText(), r.state(), flow.getFileName() + ": " + r.workflowId() + " " + d);
                                        } else {
                                            assertTrue(!d.path("error").asText("").startsWith("no handler"), flow.getFileName() + ": " + d);
                                        }
                                    }
                                    if (expect != null) {
                                        for (Map.Entry<String, JsonNode> x : expect.properties()) {
                                            assertTrue(rows.stream().anyMatch(r -> r.smType().equals(smType(x.getKey()))),
                                                    flow.getFileName() + ": no " + x.getKey() + " event in the flow's causation trees");
                                        }
                                    }
                                }
                            }
                        }
                    }

                    /** {@code OrderCreateEvent} -> {@code evt_order_create_event} (the lifecycle rows' smType). */
                    private static String smType(String eventType) {
                        return io.concert.sdk.events.EventRows.eventSmType(eventType);
                    }

                    /** Waits until the event and all of its descendants have left NEW; returns their lifecycle rows. */
                    private static List<SmStateRow> settle(InMemoryStateStore store, String rootId, String flow) {
                        long deadline = System.currentTimeMillis() + 30_000;
                        while (true) {
                            List<SmStateRow> rows = new ArrayList<>();
                            boolean settled = true;
                            Deque<String> todo = new ArrayDeque<>(List.of(rootId));
                            while (!todo.isEmpty() && settled) {
                                SmStateRow r = store.loadState("event:" + todo.pop()).orElse(null);
                                if (r == null || r.state().equals("NEW")) {
                                    settled = false;
                                    break;
                                }
                                rows.add(r);
                                Json.read(r.data(), JsonNode.class).path("children").forEach(c -> todo.add(c.asText()));
                            }
                            if (settled) {
                                return rows;
                            }
                            assertTrue(System.currentTimeMillis() < deadline, flow + ": " + rootId + " did not settle; rows so far: " + rows);
                            try {
                                Thread.sleep(25);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError("interrupted");
                            }
                        }
                    }
                }
                """;
    }
}
