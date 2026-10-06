package io.concert.sdk.events;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.model.runtime.ModelJson;
import io.concert.sdk.LockTemplates;
import java.util.List;

/**
 * Builds child events for {@link EventContext} and {@code ModelStateMachine.emit}. Deterministic (catalog lookup,
 * {@link ModelJson} output, template rendering), so it is also used inside workflow code. Child ids are
 * {@code <parentId>.<n>}, {@code n} counting from 1 in emission order.
 */
public final class EventEmissions {
    private EventEmissions() {}

    public static String childId(EventEnvelope parent, int n) {
        return parent.eventId() + "." + n;
    }

    /**
     * An event-style child.
     *
     * @param defaultDomain domain for a class without a catalog entry ({@code null}: such a class is an error)
     * @param atMillis scheduled time, 0 = due now
     */
    public static EventEnvelope event(EventEnvelope parent, int n, Object payload, String defaultDomain, long atMillis,
            long nowMillis) {
        if (payload == null) {
            throw new IllegalArgumentException("cannot emit a null event");
        }
        EventCatalog.EventType type = EventCatalogs.byClass(payload.getClass()).orElse(null);
        if (type == null && defaultDomain == null) {
            throw new IllegalArgumentException("no EventCatalog entry for " + payload.getClass().getName()
                    + " (register one with EventCatalogs.register)");
        }
        String id = childId(parent, n);
        String json = payloadJson(payload);
        String domain = type != null ? type.domain() : defaultDomain;
        String name = type != null ? type.name() : payload.getClass().getSimpleName();
        List<String> keys = type == null || type.lockTemplates().isEmpty() ? List.of()
                : LockTemplates.render(type.lockTemplates(), id, json == null ? null : Json.read(json, JsonNode.class));
        return new EventEnvelope(id, domain, id, name, keys, json, nowMillis, 0, EventEnvelope.STYLE_EVENT,
                atMillis, null, null).withParent(parent);
    }

    /** An entity-style child for {@code smType:instanceKey}. */
    public static EventEnvelope entity(EventEnvelope parent, int n, String smType, String instanceKey, String eventType,
            Object payload, List<String> extraLockKeys, long nowMillis) {
        String id = childId(parent, n);
        return new EventEnvelope(id, smType, instanceKey, eventType, extraLockKeys, payloadJson(payload), nowMillis, 0)
                .withParent(parent);
    }

    /** JSON of a payload: {@code null}, a JSON string as is, otherwise {@link ModelJson}. */
    public static String payloadJson(Object payload) {
        if (payload == null) {
            return null;
        }
        if (payload instanceof String s) {
            return s;
        }
        if (payload instanceof JsonNode n) {
            return Json.write(n);
        }
        return ModelJson.write(payload);
    }
}
