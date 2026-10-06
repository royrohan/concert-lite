package io.concert.ecosystem;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.EnumDef;
import io.concert.model.pure.Literal;
import io.concert.model.pure.PropertyDef;
import io.concert.model.pure.ResolvedModel;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Example payloads generated from the command classes: every stored property gets a value that fits
 * its type and name (amounts, prices, emails, currencies, ...), enums their default or first value,
 * nested classes an object, lists one element. Ids are consistent across the whole ecosystem: a field
 * named like a machine's id property (e.g. {@code policyId}) holds that machine's sample instance key
 * ({@code POLICY-1001}), and any other id-like field always gets the same value, so the events of a
 * flow address and lock the same entities.
 */
final class SampleGenerator {

    static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonNodeFactory F = JsonNodeFactory.instance;
    private static final int MAX_DEPTH = 3;

    private final Ecosystem eco;
    private final ResolvedModel model;
    private final Map<String, String> keysByField = new HashMap<>();

    SampleGenerator(Ecosystem eco) {
        this.eco = eco;
        this.model = eco.model();
        for (MachineDecl m : eco.machines()) {
            String key = Ecosystem.sampleKey(m);
            keysByField.putIfAbsent(Ecosystem.camel(m.smType()) + "Id", key);
            keysByField.putIfAbsent(Character.toLowerCase(m.root().simpleName().charAt(0)) + m.root().simpleName().substring(1) + "Id", key);
            if (m.idProperty() != null) {
                keysByField.put(m.idProperty(), key);
            }
        }
    }

    /** Sample payload per event type of a machine ({@code null} for untyped events), in event order. */
    Map<String, JsonNode> samples(MachineDecl m) {
        Map<String, JsonNode> out = new LinkedHashMap<>();
        for (String ev : m.eventTypes()) {
            String cls = m.payloadClass(ev);
            out.put(ev, cls == null ? null : object(model.findClass(cls).orElseThrow(), 0));
        }
        return out;
    }

    /** The happy-path flow of a machine: {@code {name, description, events: [{smType, instanceKey, eventType, payload}]}}. */
    ObjectNode happyPath(MachineDecl m) {
        Map<String, JsonNode> samples = samples(m);
        ObjectNode flow = F.objectNode();
        List<MachineDecl.Transition> path = m.happyPath();
        flow.put("name", m.smType() + "-happy-path");
        StringBuilder desc = new StringBuilder(m.initial());
        path.forEach(t -> desc.append(" -").append(t.eventType()).append("-> ").append(t.to()));
        flow.put("description", desc.toString());
        ArrayNode events = flow.putArray("events");
        for (MachineDecl.Transition t : path) {
            ObjectNode e = events.addObject();
            e.put("smType", m.smType());
            e.put("instanceKey", Ecosystem.sampleKey(m));
            e.put("eventType", t.eventType());
            JsonNode payload = samples.get(t.eventType());
            e.set("payload", payload == null ? F.nullNode() : payload.deepCopy());
        }
        return flow;
    }

    /** Sample payload of an event type. */
    ObjectNode eventSample(EventDecl e) {
        return object(e.cls(), 0);
    }

    /** Event types no other type emits (where flows start); every type if they all are emitted (a cycle). */
    List<EventDecl> rootEvents() {
        java.util.Set<String> emitted = new java.util.HashSet<>();
        eco.events().forEach(e -> emitted.addAll(e.emits()));
        List<EventDecl> roots = eco.events().stream().filter(e -> !emitted.contains(e.name())).toList();
        return roots.isEmpty() ? eco.events() : roots;
    }

    /**
     * The flow starting at {@code root}: {@code {name, description, events: [{style, eventType, payload}]}}, the
     * description being the chain its emits declare, e.g. {@code OrderCreateEvent -> (OrderAcceptedEvent ->
     * ReserveInventoryEvent | OrderRejectedEvent)}.
     */
    ObjectNode eventFlow(EventDecl root) {
        ObjectNode flow = F.objectNode();
        flow.put("name", root.name() + "-flow");
        flow.put("description", chain(root.name(), new java.util.HashSet<>()));
        ArrayNode events = flow.putArray("events");
        ObjectNode e = events.addObject();
        e.put("style", "event");
        e.put("eventType", root.name());
        e.set("payload", eventSample(root));
        return flow;
    }

    /** {@code A -> (B -> C | D)} from the emits declarations (cycles shown once). */
    String chain(String name, java.util.Set<String> seen) {
        EventDecl e = eco.event(name).orElse(null);
        if (e == null || e.emits().isEmpty() || !seen.add(name)) {
            return name;
        }
        List<String> next = e.emits().stream().map(n -> chain(n, new java.util.HashSet<>(seen))).toList();
        return name + " -> " + (next.size() == 1 ? next.getFirst() : "(" + String.join(" | ", next) + ")");
    }

    private ObjectNode object(ClassDef c, int depth) {
        ObjectNode o = F.objectNode();
        if (!c.superTypes().isEmpty() || model.classes().stream().anyMatch(x -> x.superTypes().contains(c.qualifiedName()))) {
            o.put("@type", c.qualifiedName()); // classes in a hierarchy are (de)serialized polymorphically
        }
        for (PropertyDef p : model.allProperties(c)) {
            JsonNode v = value(p, depth);
            if (v != null) {
                o.set(p.name(), v);
            }
        }
        return o;
    }

    private JsonNode value(PropertyDef p, int depth) {
        JsonNode one = single(p, depth);
        if (one == null) {
            return null;
        }
        if (p.multiplicity().isToMany()) {
            ArrayNode a = F.arrayNode();
            a.add(one);
            return a;
        }
        return one;
    }

    private JsonNode single(PropertyDef p, int depth) {
        String name = p.name();
        String lower = name.toLowerCase(Locale.ROOT);
        if (model.isEnum(p.type())) {
            EnumDef e = model.findEnum(p.type().name()).orElseThrow();
            if (p.defaultValue() instanceof Literal.EnumLit d) {
                return F.textNode(d.value());
            }
            return e.values().isEmpty() ? null : F.textNode(e.values().getFirst().name());
        }
        if (model.isClass(p.type())) {
            if (depth >= MAX_DEPTH) {
                return null;
            }
            return object(model.findClass(p.type().name()).orElseThrow(), depth + 1);
        }
        if (!p.type().primitive()) {
            return null;
        }
        return switch (p.type().primitiveType()) {
            case STRING -> F.textNode(p.defaultValue() instanceof Literal.StringLit s ? s.value() : string(name, lower));
            case INTEGER -> F.numberNode(p.defaultValue() instanceof Literal.IntLit i ? i.value() : integer(lower));
            case FLOAT, DECIMAL, NUMBER -> F.numberNode(decimal(lower));
            case BOOLEAN -> F.booleanNode(!(p.defaultValue() instanceof Literal.BoolLit b) || b.value());
            case STRICT_DATE, DATE -> F.textNode("2026-01-15");
            case DATE_TIME -> F.textNode("2026-01-15T10:00:00Z");
        };
    }

    private String string(String name, String lower) {
        String key = keysByField.get(name);
        if (key != null) {
            return key;
        }
        if (lower.endsWith("id") || lower.endsWith("ref") || lower.endsWith("account") || lower.endsWith("number")
                || lower.endsWith("code") || lower.endsWith("key")) {
            return keysByField.computeIfAbsent(name, n -> kebab(n).toUpperCase(Locale.ROOT) + "-1001");
        }
        if (lower.contains("email")) {
            return "jane.doe@example.com";
        }
        if (lower.contains("currency")) {
            return "USD";
        }
        if (lower.contains("country")) {
            return "US";
        }
        if (lower.contains("city")) {
            return "Springfield";
        }
        if (lower.contains("street") || lower.contains("address")) {
            return "1 Main St";
        }
        if (lower.contains("phone")) {
            return "+1-555-0100";
        }
        if (lower.contains("symbol")) {
            return "AAPL";
        }
        if (lower.contains("venue")) {
            return "XNAS";
        }
        if (lower.endsWith("name") || lower.endsWith("by") || lower.contains("holder") || lower.contains("assessor")) {
            return "Jane Doe";
        }
        if (lower.contains("reason") || lower.contains("note") || lower.contains("comment") || lower.contains("description")) {
            return "sample " + kebab(name).replace('-', ' ');
        }
        return kebab(name) + "-1";
    }

    private static long integer(String lower) {
        if (lower.contains("qty") || lower.contains("quantity") || lower.contains("count") || lower.contains("units")) {
            return 10;
        }
        if (lower.contains("year")) {
            return 2026;
        }
        if (lower.contains("amount") || lower.contains("cents") || lower.contains("total")) {
            return 1000;
        }
        if (lower.contains("ms") || lower.contains("millis")) {
            return 0;
        }
        return 1;
    }

    private static BigDecimal decimal(String lower) {
        if (lower.contains("price") || lower.endsWith("px")) {
            return new BigDecimal("101.25");
        }
        if (lower.contains("rate") || lower.contains("pct") || lower.contains("ratio") || lower.contains("percent")) {
            return new BigDecimal("0.05");
        }
        if (lower.contains("fee")) {
            return new BigDecimal("1.50");
        }
        if (lower.contains("amount") || lower.contains("total") || lower.contains("premium") || lower.contains("limit")
                || lower.contains("value") || lower.contains("notional") || lower.contains("deductible") || lower.contains("sum")) {
            return new BigDecimal("1250.00");
        }
        return new BigDecimal("100.00");
    }

    /** {@code payeeAccount} -> {@code payee-account}. */
    static String kebab(String camel) {
        StringBuilder sb = new StringBuilder();
        for (char c : camel.toCharArray()) {
            if (Character.isUpperCase(c) && !sb.isEmpty()) {
                sb.append('-');
            }
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }

    /** Pretty JSON with a trailing new line. */
    static String pretty(JsonNode node) {
        try {
            return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(node) + "\n";
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    Ecosystem ecosystem() {
        return eco;
    }
}
