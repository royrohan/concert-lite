package io.concert.orchestration.events;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventRouting;
import io.concert.common.Json;
import io.concert.common.api.EventLifecycle;
import io.concert.common.api.EventOutcome;
import io.concert.common.api.ProcessorStatus;
import io.concert.common.api.SmStateRow;
import io.concert.store.StateStore;
import io.temporal.client.WorkflowClient;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Operator API for event-style events: list them by lifecycle status (from their {@code event:<id>} rows in the
 * StateStore), and retry / skip blocked or parked ones (updates on their processor workflow). The trace UI and
 * {@code scripts/events.sh} build on this.
 */
public final class EventOps {

    /** One lifecycle row, flattened. */
    public record EventRow(String eventId, String eventType, String domain, EventLifecycle status, String outcome,
            String error, int attempts, String parentEventId, String causationRoot, List<String> keys,
            List<String> children, String processor, long updatedAtMillis, long version) {}

    private final WorkflowClient client;
    private final StateStore store;

    public EventOps(WorkflowClient client, StateStore store) {
        this.client = client;
        this.store = store;
    }

    /** Events in a status, oldest update first (scans; for operators, not the hot path). */
    public List<EventRow> list(EventLifecycle status) {
        return store.scanStatesWhere("event:", "status", status.name()).stream().map(EventOps::row)
                .sorted(Comparator.comparingLong(EventRow::updatedAtMillis)).toList();
    }

    /** Every event whose id starts with {@code idPrefix} ("" = all), oldest update first. */
    public List<EventRow> listByPrefix(String idPrefix) {
        return store.scanStates("event:" + idPrefix).stream().map(EventOps::row)
                .sorted(Comparator.comparingLong(EventRow::updatedAtMillis)).toList();
    }

    public Optional<EventRow> get(String eventId) {
        return store.loadState("event:" + eventId).map(EventOps::row);
    }

    /** Retries a blocked (runs now) or parked (re-enters its lock chain) event. */
    public EventOutcome retry(String eventId) {
        EventRow r = require(eventId);
        return EventRouting.processor(client, r.domain(), processorKey(r)).retry(eventId);
    }

    /** Skips a blocked or parked event: it goes DONE without running (a blocked one frees its keys). */
    public EventOutcome skip(String eventId, String reason) {
        EventRow r = require(eventId);
        return EventRouting.processor(client, r.domain(), processorKey(r)).skip(eventId, reason);
    }

    /** The live status of the processor that owns the event. */
    public ProcessorStatus status(String eventId) {
        EventRow r = require(eventId);
        return EventRouting.processor(client, r.domain(), processorKey(r)).status();
    }

    private EventRow require(String eventId) {
        return get(eventId).orElseThrow(() -> new IllegalArgumentException("no lifecycle row event:" + eventId));
    }

    private static String processorKey(EventRow r) {
        return r.keys().isEmpty() ? r.domain() + ":" + r.eventId() : r.keys().getFirst();
    }

    static EventRow row(SmStateRow s) {
        JsonNode d = s.data() == null ? Json.MAPPER.createObjectNode() : Json.read(s.data(), JsonNode.class);
        return new EventRow(
                text(d, "eventId", s.workflowId().substring("event:".length())),
                text(d, "eventType", null),
                text(d, "domain", null),
                EventLifecycle.valueOf(s.state()),
                text(d, "outcome", null),
                text(d, "error", null),
                d.path("attempts").asInt(0),
                text(d, "parentEventId", null),
                text(d, "causationRoot", null),
                strings(d.get("keys")),
                strings(d.get("children")),
                text(d, "processor", null),
                s.updatedAtMillis(),
                s.version());
    }

    private static String text(JsonNode d, String field, String dflt) {
        JsonNode n = d.get(field);
        return n == null || n.isNull() ? dflt : n.asText();
    }

    private static List<String> strings(JsonNode n) {
        if (n == null || !n.isArray()) {
            return List.of();
        }
        return java.util.stream.StreamSupport.stream(n.spliterator(), false).map(JsonNode::asText).toList();
    }
}
