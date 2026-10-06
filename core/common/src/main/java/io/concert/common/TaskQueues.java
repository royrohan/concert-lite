package io.concert.common;

public final class TaskQueues {
    private TaskQueues() {}

    /** Ingest supervisor, shard consumers, lock and event workflows (the "coordinator"). */
    public static final String ORCHESTRATION = "orchestration";

    /** One task queue per state machine type, so each type scales with its own workers. */
    public static String stateMachine(String smType) {
        return "sm-" + smType;
    }

    /** One task queue per event-style handler application ("domain"): its processors, handlers and emits. */
    public static String events(String domain) {
        return "ev-" + domain;
    }
}
