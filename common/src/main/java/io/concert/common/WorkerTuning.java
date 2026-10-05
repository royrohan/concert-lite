package io.concert.common;

import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.WorkerOptions;
import io.temporal.worker.tuning.PollerBehaviorAutoscaling;

/**
 * Shared Temporal worker settings.
 *
 * <ul>
 *   <li>Workflow threads are virtual. With platform threads the SDK caps cached workflows at the
 *       thread count (600), so thousands of lock / entity workflows get evicted and every task
 *       replays full history. Virtual threads (no pinning on JDK 24+) remove that cap.
 *   <li>Pollers autoscale. A fixed 8 pollers capped the orchestration queue at ~35 multi-key
 *       events/s in the first benchmark run.
 * </ul>
 */
public final class WorkerTuning {
    private WorkerTuning() {}

    public static WorkerFactoryOptions factoryOptions() {
        return WorkerFactoryOptions.newBuilder()
                .setUsingVirtualWorkflowThreads(true)
                .setWorkflowCacheSize(Env.getInt("WORKER_WORKFLOW_CACHE", 50_000))
                .setMaxWorkflowThreadCount(Env.getInt("WORKER_WORKFLOW_THREADS", 100_000))
                .build();
    }

    public static WorkerOptions.Builder workerOptions(int maxWorkflowPollers, int maxActivityPollers) {
        return WorkerOptions.newBuilder()
                .setUsingVirtualThreads(true)
                .setStickyQueueScheduleToStartTimeout(Failover.stickyScheduleToStart())
                .setWorkflowTaskPollersBehavior(new PollerBehaviorAutoscaling(2, maxWorkflowPollers, 8))
                .setActivityTaskPollersBehavior(new PollerBehaviorAutoscaling(1, maxActivityPollers, 2))
                .setMaxConcurrentWorkflowTaskExecutionSize(Env.getInt("WORKER_WF_SLOTS", 1000))
                .setMaxConcurrentLocalActivityExecutionSize(Env.getInt("WORKER_LA_SLOTS", 2000));
    }
}
