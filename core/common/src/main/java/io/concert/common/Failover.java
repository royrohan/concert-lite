package io.concert.common;

import java.time.Duration;

/**
 * How fast Temporal moves work off a crashed process. Defaults are tuned from the chaos runs: with
 * Temporal's stock values a kill -9 stalled affected keys ~10 s even with a healthy twin running.
 *
 * <ul>
 *   <li>workflow task timeout (stock 10 s): an in-flight task on a dead worker is retried elsewhere
 *       after this. Long local-activity chains are unaffected: the SDK heartbeats the task at 80%.
 *   <li>sticky schedule-to-start (stock 5 s): a cached workflow's next task waits this long on the
 *       dead worker's sticky queue before falling back to the shared queue.
 *   <li>shard heartbeat timeout: a dead coordinator's shard consumers restart elsewhere after this.
 * </ul>
 *
 * Lower = faster failover, but a worker paused longer than this (GC, CPU starvation) loses its
 * tasks to another worker and some work is redone (always safely: everything is idempotent).
 */
public final class Failover {
    private Failover() {}

    public static Duration workflowTaskTimeout() {
        return Duration.ofMillis(Env.getLong("WORKFLOW_TASK_TIMEOUT_MS", 2000));
    }

    public static Duration stickyScheduleToStart() {
        return Duration.ofMillis(Env.getLong("STICKY_SCHEDULE_TO_START_MS", 1000));
    }

    public static int shardHeartbeatTimeoutSeconds() {
        return Env.getInt("SHARD_HEARTBEAT_TIMEOUT_SEC", 3);
    }
}
