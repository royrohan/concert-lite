package io.concert.sdk.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EventLifecycle;
import java.util.List;
import java.util.Locale;

/**
 * Naming and JSON of the rows event-style processing keeps in the StateStore ({@code sm_state}):
 *
 * <ul>
 *   <li>lifecycle row {@code event:<eventId>}, smType {@code evt_<snake eventType>}, state = the
 *       {@link EventLifecycle} status, data = {@link #lifecycle};
 *   <li>keyed-state document {@code state:<key>}, smType {@code st_<snake stateType>}, state {@value #STATE_CURRENT},
 *       data = the model JSON.
 * </ul>
 */
public final class EventRows {
    private EventRows() {}

    public static final String EVENT_PREFIX = "event:";
    public static final String STATE_PREFIX = "state:";
    public static final String STATE_CURRENT = "CURRENT";

    public static String eventRowId(String eventId) {
        return EVENT_PREFIX + eventId;
    }

    public static String stateRowId(String key) {
        return STATE_PREFIX + key;
    }

    public static String eventSmType(String eventType) {
        return "evt_" + snake(eventType);
    }

    public static String stateSmType(String stateType) {
        return "st_" + snake(stateType);
    }

    /** {@code OrderCreateEvent} -> {@code order_create_event}; other characters become {@code _}. */
    public static String snake(String name) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0 && sb.length() > 0 && sb.charAt(sb.length() - 1) != '_'
                        && (Character.isLowerCase(name.charAt(i - 1)) || Character.isDigit(name.charAt(i - 1))
                                || (i + 1 < name.length() && Character.isLowerCase(name.charAt(i + 1))))) {
                    sb.append('_');
                }
                sb.append(Character.toLowerCase(c));
            } else if (Character.isLetterOrDigit(c)) {
                sb.append(c);
            } else if (sb.length() > 0 && sb.charAt(sb.length() - 1) != '_') {
                sb.append('_');
            }
        }
        return sb.toString().toLowerCase(Locale.ROOT);
    }

    /**
     * Data of a lifecycle row: {@code {eventId, eventType, domain, status, outcome, error, attempts, retries,
     * parentEventId, causationRoot, depth, processor, keys, requestId, scheduledAt, children, emitted, payload}}.
     * Deterministic, so it can be built in workflow code.
     */
    public static String lifecycle(EventEnvelope e, EventLifecycle status, String outcome, String error, int attempts,
            int retries, String requestId, List<EventEnvelope> emitted) {
        ObjectNode n = Json.MAPPER.createObjectNode();
        n.put("eventId", e.eventId());
        n.put("eventType", e.eventType());
        n.put("domain", e.domain());
        n.put("status", status.name());
        n.put("outcome", outcome);
        if (error != null) {
            n.put("error", error);
        }
        n.put("attempts", attempts);
        n.put("retries", retries);
        if (e.parentEventId() != null) {
            n.put("parentEventId", e.parentEventId());
        }
        n.put("causationRoot", e.causationRoot() != null ? e.causationRoot() : e.eventId());
        n.put("depth", depth(e));
        n.put("processor", WorkflowIds.processor(e.domain(), e.processorKey()));
        n.put("processorKey", e.processorKey());
        ArrayNode keys = n.putArray("keys");
        e.effectiveLockKeys().forEach(keys::add);
        if (requestId != null) {
            n.put("requestId", requestId);
        }
        if (e.scheduledAtMillis() > 0) {
            n.put("scheduledAt", e.scheduledAtMillis());
        }
        ArrayNode children = n.putArray("children");
        emitted.forEach(c -> children.add(c.eventId()));
        if (!emitted.isEmpty()) {
            n.set("emitted", Json.MAPPER.valueToTree(emitted));
        }
        n.set("payload", payloadNode(e.payload()));
        return Json.write(n);
    }

    /**
     * How far down its causation chain an event is: 0 for an event sent from outside, else the number of
     * {@code .<n>} steps from the causation root (child ids are {@code <parentId>.<n>}), at least 1.
     */
    public static int depth(EventEnvelope e) {
        if (e.parentEventId() == null) {
            return 0;
        }
        String root = e.causationRoot();
        if (root != null && e.eventId().startsWith(root + ".")) {
            return (int) e.eventId().substring(root.length()).chars().filter(c -> c == '.').count();
        }
        return 1;
    }

    /** The {@code emitted} child envelopes recorded in lifecycle row data. */
    public static List<EventEnvelope> emitted(String lifecycleData) {
        if (lifecycleData == null) {
            return List.of();
        }
        JsonNode em = Json.read(lifecycleData, JsonNode.class).get("emitted");
        if (em == null || !em.isArray()) {
            return List.of();
        }
        return Json.MAPPER.convertValue(em, Json.MAPPER.getTypeFactory().constructCollectionType(List.class, EventEnvelope.class));
    }

    private static JsonNode payloadNode(String payload) {
        if (payload == null) {
            return Json.MAPPER.nullNode();
        }
        try {
            return Json.MAPPER.readTree(payload);
        } catch (Exception notJson) {
            return Json.MAPPER.getNodeFactory().textNode(payload);
        }
    }
}
