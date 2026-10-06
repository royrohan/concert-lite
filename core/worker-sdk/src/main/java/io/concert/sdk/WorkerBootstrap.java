package io.concert.sdk;

import io.concert.common.Env;
import io.concert.common.TaskQueues;
import io.concert.common.WorkerTuning;
import io.concert.sdk.events.EventEnqueueActivities;
import io.concert.sdk.events.EventEnqueueActivitiesImpl;
import io.concert.store.StateStore;
import io.concert.store.TraceWriter;
import io.temporal.client.WorkflowClient;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.concert.sdk.reconcile.DuckDbSinkVersions;
import io.concert.sdk.reconcile.ReconcileActivitiesImpl;
import io.concert.sdk.reconcile.ReconcileSchedules;
import io.concert.sdk.reconcile.ReconcileWorkflow;
import io.concert.sdk.reconcile.ReconcileWorkflowImpl;
import io.concert.sdk.reconcile.Reconciler;
import java.time.Duration;
import java.util.Arrays;
import org.slf4j.LoggerFactory;

/**
 * Starts a Temporal worker for one state machine type on task queue {@code sm-<type>}. Besides the
 * persistence activities and the {@link EventEnqueueActivities} behind {@code ModelStateMachine.emit}, it registers an {@link EntitySnapshotPublisher} (used by typed machines on
 * terminal transitions): the one passed in {@code extraActivities}, else
 * {@link EntitySnapshotPublisher#fromEnv()}. Typed machines also get reconciliation for their type
 * ({@link ReconcileWorkflow}; with Kafka configured, the Temporal Schedule {@code reconcile-<type>} runs it
 * every {@code RECONCILE_INTERVAL_MINUTES} (60), unless {@code RECONCILE_SCHEDULE=false}).
 */
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
        if (Arrays.stream(extraActivities).noneMatch(a -> a instanceof EventEnqueueActivities)) {
            // ModelStateMachine.emit: entity transitions emitting event-style (or other entity) events
            worker.registerActivitiesImplementations(new EventEnqueueActivitiesImpl(client, new TraceWriter(store)));
        }
        EntitySnapshotPublisher publisher = Arrays.stream(extraActivities)
                .filter(a -> a instanceof EntitySnapshotPublisher).map(a -> (EntitySnapshotPublisher) a)
                .findFirst().orElse(null);
        if (publisher == null) {
            publisher = EntitySnapshotPublisher.fromEnv();
            worker.registerActivitiesImplementations(publisher);
        }
        boolean reconcile = registerReconcile(worker, store, smType, impl, publisher);
        factory.start();
        if (reconcile && publisher != EntitySnapshotPublisher.logging() && Env.getBool("RECONCILE_SCHEDULE", true)) {
            ReconcileSchedules.ensure(client, smType, Duration.ofMinutes(Env.getLong("RECONCILE_INTERVAL_MINUTES", 60)));
        }
        return factory;
    }

    /**
     * Typed machines (the ones that publish snapshots) also get the reconciliation workflow and activity
     * for their own smType on their task queue; see {@link ReconcileWorkflow}. The sink is the DuckDB
     * sink at {@code SINK_DUCKDB_URL}.
     */
    private static boolean registerReconcile(Worker worker, StateStore store, String smType,
            Class<? extends AbstractStateMachine> impl, EntitySnapshotPublisher publisher) {
        if (!ModelStateMachine.class.isAssignableFrom(impl)) {
            return false;
        }
        StateMachineSpec spec = StateMachineRegistry.get(smType);
        if (spec == null) {
            MachineCatalog.Machine m = MachineCatalog.all().get(smType);
            spec = m == null ? null : m.spec();
        }
        if (spec == null) {
            LoggerFactory.getLogger(WorkerBootstrap.class).warn(
                    "no spec for {} (StateMachineRegistry / MachineCatalog): reconciliation not available", smType);
            return false;
        }
        Reconciler reconciler = new Reconciler(store, spec,
                new DuckDbSinkVersions(Env.get("SINK_DUCKDB_URL", "http://localhost:8090")), publisher);
        String unavailable = publisher == EntitySnapshotPublisher.logging()
                ? "KAFKA_BOOTSTRAP is not set: snapshots cannot be published" : null;
        worker.registerWorkflowImplementationTypes(ReconcileWorkflowImpl.class);
        worker.registerActivitiesImplementations(new ReconcileActivitiesImpl(smType, reconciler, unavailable));
        return true;
    }

}
