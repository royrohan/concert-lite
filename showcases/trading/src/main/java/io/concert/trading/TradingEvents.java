package io.concert.trading;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.concert.model.runtime.ModelJson;
import io.concert.trading.command.OrderEvent;
import java.util.List;

/**
 * Builds concert events from the generated command classes, addressed and locked exactly as
 * {@link TradingFlows} says. Payload JSON is {@code ModelJson} output (sorted properties, ISO-8601
 * instants, plain decimals, nulls omitted), the form {@code ModelStateMachine} binds.
 */
public final class TradingEvents {
    private TradingEvents() {}

    private static final ObjectMapper LOOKUP = ModelJson.mapper();

    /**
     * The event {@code eventType} for the {@code smType} instance named by the payload's key field,
     * with the flow's lock keys rendered from the payload.
     *
     * @throws IllegalArgumentException if the payload is not the flow's payload class for that event
     *     or lacks a field the key or a lock key needs
     */
    public static EventEnvelope envelope(String smType, String eventType, String eventId, OrderEvent payload,
            long tsMillis) {
        TradingFlows.Flow flow = TradingFlows.flow(smType);
        String expected = flow.payloadClass(eventType);
        if (!payload.getClass().getSimpleName().equals(expected)) {
            throw new IllegalArgumentException(smType + "." + eventType + " takes " + expected + ", got "
                    + payload.getClass().getSimpleName());
        }
        // The tree is only for field lookup (it may normalize decimals); the payload is written as is.
        JsonNode tree = LOOKUP.valueToTree(payload);
        String key = text(tree, flow.keyField());
        if (key == null) {
            throw new IllegalArgumentException(expected + " has no " + flow.keyField());
        }
        List<String> locks = flow.renderLocks(eventType, f -> text(tree, f));
        return new EventEnvelope(eventId, smType, key, eventType, locks, ModelJson.write(payload), tsMillis, 0);
    }

    /** Kinesis partition key: the first sorted lock key, on which the platform keeps per-shard order. */
    public static String partitionKey(EventEnvelope e) {
        return e.effectiveLockKeys().getFirst();
    }

    private static String text(JsonNode tree, String field) {
        JsonNode n = tree.get(field);
        return n == null || n.isNull() ? null : n.asText();
    }
}
