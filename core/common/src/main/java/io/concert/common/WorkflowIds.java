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

    /** Lock workflow of a key in this process's flow: {@code lock:<key>}, or {@code lock:<flow>:<key>} ({@link Flows}). */
    public static String lock(String lockKey) {
        return Flows.lockWorkflowId(Flows.CURRENT, lockKey);
    }

    public static String event(String eventId) {
        return "evt:" + eventId;
    }

    /** Event-style processor of a domain and key, e.g. {@code evproc:orders:order:7}. */
    public static String processor(String domain, String key) {
        return "evproc:" + domain + ":" + key;
    }

    public static String ingest(String stream) {
        return "ingest:" + stream;
    }
}
