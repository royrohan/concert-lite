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
import java.util.Map;
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

    /** As {@link #start}; {@code activitySlots} caps concurrent transitions in this worker (benchmarks). */
    public static WorkerFactory startWithSlots(
            WorkflowClient client, StateStore store, String smType, Class<? extends AbstractStateMachine> impl,
            int activitySlots, Object... extraActivities) {
        System.setProperty("WORKER_ACTIVITY_SLOTS", Integer.toString(activitySlots));
        return start(client, store, smType, impl, extraActivities);
    }

    public static WorkerFactory start(
            WorkflowClient client, StateStore store, String smType, Class<? extends AbstractStateMachine> impl,
            Object... extraActivities) {
        WorkerFactory factory = WorkerFactory.newInstance(client, WorkerTuning.factoryOptions());
        // Activities only: the ApplyTransition activity is the type's whole entity path (no entity workflows).
        Worker worker = factory.newWorker(TaskQueues.stateMachine(smType), WorkerTuning.workerOptions(
                        Env.getInt("WORKER_WF_POLLERS_MAX", 64), Env.getInt("WORKER_ACTIVITY_POLLERS_MAX", 64))
                .setMaxConcurrentActivityExecutionSize(Env.getInt("WORKER_ACTIVITY_SLOTS", 1000))
                .build());
        TraceWriter traces = new TraceWriter(store);
        if (extraActivities.length > 0) {
            worker.registerActivitiesImplementations(extraActivities);
        }
        EventEnqueueActivities enqueue = Arrays.stream(extraActivities).filter(a -> a instanceof EventEnqueueActivities)
                .map(a -> (EventEnqueueActivities) a).findFirst().orElse(null);
        if (enqueue == null) {
            // outbox delivery of the events transitions emit (event-style or other entities)
            enqueue = new EventEnqueueActivitiesImpl(client, traces);
            worker.registerActivitiesImplementations(enqueue);
        }
        EntitySnapshotPublisher publisher = Arrays.stream(extraActivities)
                .filter(a -> a instanceof EntitySnapshotPublisher).map(a -> (EntitySnapshotPublisher) a)
                .findFirst().orElse(null);
        if (publisher == null) {
            publisher = EntitySnapshotPublisher.fromEnv();
            worker.registerActivitiesImplementations(publisher);
        }
        worker.registerActivitiesImplementations(
                new TransitionActivitiesImpl(store, Map.of(smType, impl), enqueue, publisher, traces));
        OutboxSweeper.start(store, smType, enqueue);
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
