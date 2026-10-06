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
        FAILED,
        // event style (appended: stored by name, so older rows and readers are unaffected)
        /** Waiting for its {@code scheduledAt} in the processor; holds no locks. */
        SCHEDULED,
        /** The handler failed with a blocking error: the event keeps its lock keys until retried or skipped. */
        BLOCKED,
        /** The handler failed with a non-blocking error: the event is parked and its keys are released. */
        PARKED,
        /** A handler (or entity transition) emitted this event; detail names the parent. */
        EMITTED
    }
}
