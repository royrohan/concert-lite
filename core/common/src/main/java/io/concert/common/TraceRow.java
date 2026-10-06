package io.concert.common;

/**
 * One step in an event's life, persisted to {@code event_trace} and shown by the trace UI.
 *
 * @param workflowId the entity workflow the event targets (for entity-history lookups)
 * @param detail free text, e.g. {@code CREATED -> PAID} or {@code waitedMs=12}
 */
public record TraceRow(String eventId, long tsMillis, Stage stage, String workflowId, String lockKey, String detail) {

    public enum Stage {
        RECEIVED,
        DROPPED,
        LOCK_WAIT,
        LOCK_GRANTED,
        TRANSITION,
        REJECTED,
        RELEASED,
        DONE,
        FAILED
    }
}
