package io.concert.orchestration;

import io.concert.common.Failover;
import io.concert.common.TaskQueues;
import io.concert.common.WorkerTuning;
import io.concert.common.WorkflowIds;
import io.concert.common.api.IngestConfig;
import io.concert.common.api.IngestSupervisorWorkflow;
import io.concert.store.StateStore;
import io.concert.store.TraceWriter;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import java.util.Set;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/**
 * The "coordinator": a Temporal worker on the orchestration task queue hosting the ingest
 * supervisor, shard consumers, lock and event workflows. Run as many as you like; Temporal spreads
 * the work and fails it over.
 */
public final class OrchestrationWorker implements AutoCloseable {

    public record Settings(
            Set<String> directSmTypes,
            int maxParallelGroups,
            long idlePollMillis,
            long checkpointEveryMillis,
            int workflowPollers,
            int activityPollers) {

        public static Settings defaults() {
            return new Settings(Set.of(), 256, 20, 500, 64, 8);
        }
    }

    private final WorkflowClient client;
    private final WorkerFactory factory;
    private final TraceWriter traces;
    private final IngestDispatcher dispatcher;

    public OrchestrationWorker(WorkflowClient client, StateStore store, KinesisClient kinesis, Settings s) {
        this.client = client;
        this.traces = new TraceWriter(store);
        DispatchActivitiesImpl dispatch = new DispatchActivitiesImpl(client, traces);
        this.dispatcher = new IngestDispatcher(store, traces, dispatch, s.directSmTypes(), s.maxParallelGroups());

        this.factory = WorkerFactory.newInstance(client, WorkerTuning.factoryOptions());
        Worker worker = factory.newWorker(TaskQueues.ORCHESTRATION,
                WorkerTuning.workerOptions(s.workflowPollers(), s.activityPollers()).build());
        worker.registerWorkflowImplementationTypes(
                KeyLockWorkflowImpl.class, IngestSupervisorWorkflowImpl.class);
        worker.registerActivitiesImplementations(
                dispatch,
                new ShardConsumerActivitiesImpl(kinesis, store, dispatcher, s.idlePollMillis(), s.checkpointEveryMillis()));
    }

    public OrchestrationWorker start() {
        factory.start();
        return this;
    }

    /** Idempotently starts the supervisor for a stream (one per stream cluster-wide). */
    public void ensureIngest(IngestConfig config) {
        IngestSupervisorWorkflow wf = client.newWorkflowStub(IngestSupervisorWorkflow.class, WorkflowOptions.newBuilder()
                .setWorkflowId(WorkflowIds.ingest(config.stream()))
                .setTaskQueue(TaskQueues.ORCHESTRATION)
                .setWorkflowTaskTimeout(Failover.workflowTaskTimeout())
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                .build());
        try {
            WorkflowClient.start(wf::run, config);
        } catch (WorkflowExecutionAlreadyStarted ignored) {
            // already running
        }
    }

    public IngestDispatcher dispatcher() {
        return dispatcher;
    }

    public TraceWriter traces() {
        return traces;
    }

    @Override
    public void close() {
        // Interrupt long-running shard consumers: they checkpoint and fail, and Temporal retries
        // them on another orchestration worker right away instead of after a heartbeat timeout.
        factory.shutdownNow();
        factory.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        dispatcher.close();
        traces.close();
    }
}
