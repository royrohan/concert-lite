package io.concert.sdk;

import io.concert.common.Env;
import io.concert.common.TaskQueues;
import io.concert.common.WorkerTuning;
import io.concert.store.StateStore;
import io.temporal.client.WorkflowClient;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

/** Starts a Temporal worker for one state machine type on task queue {@code sm-<type>}. */
public final class WorkerBootstrap {
    private WorkerBootstrap() {}

    public static WorkerFactory start(
            WorkflowClient client, StateStore store, String smType, Class<? extends AbstractStateMachine> impl,
            Object... extraActivities) {
        return startWithSlots(client, store, smType, impl, Env.getInt("WORKER_LA_SLOTS", 2000), extraActivities);
    }

    /**
     * @param localActivitySlots max concurrent local activities (persistence + {@code onTransition}
     *     work) in this worker; with simulated work it sets the worker's capacity
     */
    public static WorkerFactory startWithSlots(
            WorkflowClient client, StateStore store, String smType, Class<? extends AbstractStateMachine> impl,
            int localActivitySlots, Object... extraActivities) {
        WorkerFactory factory = WorkerFactory.newInstance(client, WorkerTuning.factoryOptions());
        Worker worker = factory.newWorker(TaskQueues.stateMachine(smType), WorkerTuning.workerOptions(
                        Env.getInt("WORKER_WF_POLLERS_MAX", 64), Env.getInt("WORKER_ACTIVITY_POLLERS_MAX", 4))
                .setMaxConcurrentLocalActivityExecutionSize(localActivitySlots)
                .build());
        worker.registerWorkflowImplementationTypes(impl);
        worker.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store));
        if (extraActivities.length > 0) {
            worker.registerActivitiesImplementations(extraActivities);
        }
        factory.start();
        return factory;
    }

}
