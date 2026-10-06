package io.concert.common;

/** Deterministic Temporal workflow ids; they double as idempotency keys. */
public final class WorkflowIds {
    private WorkflowIds() {}

    public static String entityKey(String smType, String instanceKey) {
        return smType + ":" + instanceKey;
    }

    /** Entity workflow id == its lock key, e.g. {@code order:123}. */
    public static String entity(String smType, String instanceKey) {
        return entityKey(smType, instanceKey);
    }

    public static String lock(String lockKey) {
        return "lock:" + lockKey;
    }

    public static String event(String eventId) {
        return "evt:" + eventId;
    }

    public static String ingest(String stream) {
        return "ingest:" + stream;
    }
}
