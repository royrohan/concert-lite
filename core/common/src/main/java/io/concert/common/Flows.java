package io.concert.common;

/**
 * Per-flow isolation. A flow is an ecosystem (catalog group: {@code samples}, {@code trading}, {@code insurance}, ...)
 * whose processes all set {@code CONCERT_FLOW=<flow>}. Each flow gets its own Kinesis stream, Kafka snapshot topic,
 * orchestration task queue and lock workflow ids, so two flows never share a shard, a partition, a poller or a lock,
 * even when they use the same lock key names. Without {@code CONCERT_FLOW} a process uses the legacy shared names
 * ({@code concert-events}, {@code entity-snapshots}, {@code orchestration}, {@code lock:<key>}), which tests and
 * single-flow setups rely on.
 */
public final class Flows {
    private Flows() {}

    /** The flow of this process ({@code CONCERT_FLOW}), or {@code ""} for the legacy shared setup. */
    public static final String CURRENT = Env.get("CONCERT_FLOW", "").trim();

    /** Prefixes of the per-flow names, so tools can list every flow's resources. */
    public static final String STREAM_PREFIX = "concert-";
    public static final String TOPIC_PREFIX = "snapshots.";
    public static final String QUEUE_PREFIX = TaskQueues.ORCHESTRATION + "-";

    public static String stream(String flow) {
        return flow.isEmpty() ? "concert-events" : STREAM_PREFIX + flow;
    }

    public static String snapshotTopic(String flow) {
        return flow.isEmpty() ? "entity-snapshots" : TOPIC_PREFIX + flow;
    }

    public static String orchestrationQueue(String flow) {
        return flow.isEmpty() ? TaskQueues.ORCHESTRATION : QUEUE_PREFIX + flow;
    }

    public static String lockWorkflowId(String flow, String lockKey) {
        return flow.isEmpty() ? "lock:" + lockKey : "lock:" + flow + ":" + lockKey;
    }

    /** This process's ingest stream: {@code INGEST_STREAM} if set, else the flow's stream. */
    public static String stream() {
        return Env.get("INGEST_STREAM", stream(CURRENT));
    }

    /** This process's snapshot topic: {@code SNAPSHOT_TOPIC} if set, else the flow's topic. */
    public static String snapshotTopic() {
        return Env.get("SNAPSHOT_TOPIC", snapshotTopic(CURRENT));
    }
}
