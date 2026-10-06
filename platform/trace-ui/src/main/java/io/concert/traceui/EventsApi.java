package io.concert.traceui;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Json;
import io.concert.common.api.EventLifecycle;
import io.concert.common.api.SmStateRow;
import io.concert.orchestration.events.EventOps;
import io.concert.sdk.events.EventCatalog;
import io.concert.sdk.events.EventCatalogs;
import io.concert.store.StateStore;
import io.temporal.client.WorkflowClient;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * The trace UI's event-style views: event types of every {@link EventCatalog} on the class path (each generated
 * ecosystem provides its catalogs through the SPI), an event's lifecycle row and causation tree, the operator lists
 * (errors, scheduled) and retry / skip through {@link EventOps}.
 */
final class EventsApi {

    /** Nodes of a causation tree shown at most (big fan-outs are cut). */
    static final int TREE_LIMIT = 300;

    private final StateStore store;
    private final EventOps ops;

    EventsApi(WorkflowClient client, StateStore store) {
        this.store = store;
        this.ops = new EventOps(client, store);
    }

    /** {@code {catalogs: [{name, events, states, flowMermaid, modelMermaid}]}}. */
    Map<String, Object> types() {
        List<Map<String, Object>> catalogs = new ArrayList<>();
        for (EventCatalog c : EventCatalogs.all().stream().sorted(Comparator.comparing(EventCatalog::name)).toList()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", c.name());
            m.put("events", c.events().stream().map(t -> type(c, t)).toList());
            m.put("states", c.states().stream().map(s -> {
                Map<String, Object> x = new LinkedHashMap<>();
                x.put("name", s.name());
                x.put("class", s.type().getName());
                x.put("keyTemplate", s.keyTemplate());
                return x;
            }).toList());
            m.put("flowMermaid", c.flowMermaid());
            m.put("modelMermaid", c.modelMermaid());
            catalogs.add(m);
        }
        return Map.of("catalogs", catalogs);
    }

    private static Map<String, Object> type(EventCatalog c, EventCatalog.EventType t) {
        Map<String, Object> x = new LinkedHashMap<>();
        x.put("name", t.name());
        x.put("domain", t.domain());
        x.put("payloadClass", t.payloadType().getName());
        x.put("lockTemplates", t.lockTemplates());
        x.put("onError", t.onError().name());
        x.put("maxAttempts", t.maxAttempts());
        x.put("emits", c.emits().getOrDefault(t.name(), List.of()));
        return x;
    }

    /** The catalog entry and catalog of an event type (first match by domain and name). */
    private static Optional<Map.Entry<EventCatalog, EventCatalog.EventType>> find(String domain, String name) {
        for (EventCatalog c : EventCatalogs.all()) {
            for (EventCatalog.EventType t : c.events()) {
                if (t.name().equals(name) && (domain == null || t.domain().equals(domain))) {
                    return Optional.of(Map.entry(c, t));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Event-style parts of the event page (empty for an entity-style event): {@code lifecycle} (row fields, payload,
     * scheduledAt), {@code type} (catalog entry), {@code flowMermaid}, {@code tree} (the causation tree from its root).
     */
    Map<String, Object> event(String eventId) {
        Map<String, Object> out = new LinkedHashMap<>();
        Optional<SmStateRow> row = store.loadState("event:" + eventId);
        if (row.isEmpty()) {
            return out;
        }
        JsonNode d = data(row.get());
        Map<String, Object> lc = new LinkedHashMap<>();
        lc.put("row", ops.get(eventId).orElse(null));
        lc.put("payload", d.get("payload"));
        lc.put("retries", d.path("retries").asInt(0));
        lc.put("depth", d.path("depth").asInt(0));
        lc.put("requestId", text(d, "requestId"));
        lc.put("scheduledAt", d.hasNonNull("scheduledAt") ? d.get("scheduledAt").asLong() : null);
        out.put("lifecycle", lc);
        find(text(d, "domain"), text(d, "eventType")).ifPresent(e -> {
            out.put("type", type(e.getKey(), e.getValue()));
            out.put("flowMermaid", e.getKey().flowMermaid());
        });
        String root = Optional.ofNullable(text(d, "causationRoot")).orElse(eventId);
        int[] budget = {TREE_LIMIT};
        out.put("tree", node(root, eventId, budget, 0));
        return out;
    }

    private Map<String, Object> node(String id, String focus, int[] budget, int depth) {
        Map<String, Object> n = new LinkedHashMap<>();
        n.put("eventId", id);
        n.put("focus", id.equals(focus));
        budget[0]--;
        Optional<SmStateRow> row = store.loadState("event:" + id);
        if (row.isEmpty()) {
            n.put("status", "PENDING"); // emitted, not yet seen by its processor
            n.put("children", List.of());
            return n;
        }
        JsonNode d = data(row.get());
        n.put("eventType", text(d, "eventType"));
        n.put("domain", text(d, "domain"));
        n.put("status", row.get().state());
        n.put("error", text(d, "error"));
        n.put("attempts", d.path("attempts").asInt(0));
        List<Map<String, Object>> children = new ArrayList<>();
        for (JsonNode c : d.path("children")) {
            if (budget[0] <= 0 || depth > 50) {
                n.put("truncated", true);
                break;
            }
            children.add(node(c.asText(), focus, budget, depth + 1));
        }
        n.put("children", children);
        return n;
    }

    /** {@code status}: a lifecycle status, {@code errors} (both ERROR_*), or {@code all}; newest first, at most {@code limit}. */
    Map<String, Object> list(String status, int limit) {
        List<EventOps.EventRow> rows = new ArrayList<>();
        String s = status == null || status.isBlank() ? "errors" : status;
        if (s.equalsIgnoreCase("errors")) {
            rows.addAll(ops.list(EventLifecycle.ERROR_BLOCKING));
            rows.addAll(ops.list(EventLifecycle.ERROR_NON_BLOCKING));
        } else if (s.equalsIgnoreCase("all")) {
            rows.addAll(ops.listByPrefix(""));
        } else {
            rows.addAll(ops.list(EventLifecycle.valueOf(s.toUpperCase(Locale.ROOT))));
        }
        rows.sort(Comparator.comparingLong(EventOps.EventRow::updatedAtMillis).reversed());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", s);
        out.put("total", rows.size());
        out.put("events", rows.size() > limit ? rows.subList(0, limit) : rows);
        return out;
    }

    Object retry(String eventId) {
        return ops.retry(eventId);
    }

    Object skip(String eventId, String reason) {
        return ops.skip(eventId, reason == null || reason.isBlank() ? "skipped in the trace UI" : reason);
    }

    Object processor(String eventId) {
        return ops.status(eventId);
    }

    private static JsonNode data(SmStateRow r) {
        return r.data() == null ? Json.MAPPER.createObjectNode() : Json.read(r.data(), JsonNode.class);
    }

    private static String text(JsonNode d, String f) {
        JsonNode n = d.get(f);
        return n == null || n.isNull() ? null : n.asText();
    }
}
