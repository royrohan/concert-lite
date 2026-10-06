package io.concert.orchestration;

import io.concert.common.EventEnvelope;
import io.concert.common.Failover;
import io.concert.common.SearchAttrs;
import io.concert.common.TaskQueues;
import io.concert.common.TraceRow;
import io.concert.common.WorkflowIds;
import io.concert.common.api.DispatchActivities;
import io.concert.common.api.EntityInit;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.KeyLockWorkflow;
import io.concert.common.api.LockRequest;
import io.concert.common.api.LockState;
import io.concert.common.api.TransitionResult;
import io.concert.store.TraceWriter;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.client.UpdateOptions;
import io.temporal.client.WithStartWorkflowOperation;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowException;
import io.temporal.client.WorkflowUpdateException;
import io.temporal.client.WorkflowUpdateHandle;
import io.temporal.client.WorkflowUpdateStage;
import io.temporal.failure.ApplicationFailure;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client-side calls into Temporal on behalf of workflows (local activities) and the ingest path. */
public final class DispatchActivitiesImpl implements DispatchActivities {
    private static final Logger log = LoggerFactory.getLogger(DispatchActivitiesImpl.class);
    private static final int LOCK_IDLE_SECONDS = io.concert.common.Env.getInt("LOCK_IDLE_SECONDS", 600);

    private final WorkflowClient client;
    private final TraceWriter traces;

    public DispatchActivitiesImpl(WorkflowClient client, TraceWriter traces) {
        this.client = client;
        this.traces = traces;
    }

    @Override
    public TransitionResult dispatchToEntity(EventEnvelope event, Map<String, Long> lockWaitMs) {
        long now = System.currentTimeMillis();
        lockWaitMs.forEach((key, waited) -> traces.add(new TraceRow(
                event.eventId(), now, TraceRow.Stage.LOCK_GRANTED, event.entityKey(), key, "waitedMs=" + waited)));
        List<String> keys = event.effectiveLockKeys();
        OverlapProbe.enter(keys);
        try {
            TransitionResult r = updateEntity(event, WorkflowUpdateStage.COMPLETED).getResult();
            traceDone(event, r);
            return r;
        } catch (WorkflowUpdateException e) {
            // Validator rejection: deterministic, retrying cannot help.
            throw ApplicationFailure.newNonRetryableFailure(
                    String.valueOf(e.getCause() != null ? e.getCause().getMessage() : e.getMessage()),
                    EntityRejectedException.TYPE);
        } finally {
            OverlapProbe.exit(keys);
        }
    }

    /**
     * Direct path (types configured as never sharing keys with multi-key events): returns once the
     * entity has accepted the update, which fixes its order; completion is traced asynchronously.
     */
    public CompletableFuture<TransitionResult> dispatchDirect(EventEnvelope event) {
        WorkflowUpdateHandle<TransitionResult> handle = updateEntity(event, WorkflowUpdateStage.ACCEPTED);
        return handle.getResultAsync().whenComplete((r, err) -> {
            if (err == null) {
                traceDone(event, r);
            } else {
                traces.add(new TraceRow(event.eventId(), System.currentTimeMillis(), TraceRow.Stage.FAILED,
                        event.entityKey(), null, String.valueOf(err.getMessage())));
            }
        });
    }

    private WorkflowUpdateHandle<TransitionResult> updateEntity(EventEnvelope event, WorkflowUpdateStage stage) {
        EntityWorkflow entity = client.newWorkflowStub(EntityWorkflow.class, WorkflowOptions.newBuilder()
                .setWorkflowId(WorkflowIds.entity(event.smType(), event.instanceKey()))
                .setTaskQueue(TaskQueues.stateMachine(event.smType()))
                .setWorkflowTaskTimeout(Failover.workflowTaskTimeout())
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                .setTypedSearchAttributes(SearchAttrs.forEntity(event.smType(), event.instanceKey()))
                .build());
        WithStartWorkflowOperation<Void> start =
                new WithStartWorkflowOperation<>(entity::run, EntityInit.fresh(event.smType(), event.instanceKey()));
        return WorkflowClient.startUpdateWithStart(
                entity::handle,
                event,
                UpdateOptions.<TransitionResult>newBuilder()
                        .setUpdateId(event.eventId())
                        .setWaitForStage(stage)
                        .build(),
                start);
    }

    @Override
    public void enqueueLock(String lockKey, LockRequest request) {
        for (int attempt = 1; ; attempt++) {
            KeyLockWorkflow lock = client.newWorkflowStub(KeyLockWorkflow.class, WorkflowOptions.newBuilder()
                    .setWorkflowId(WorkflowIds.lock(lockKey))
                    .setTaskQueue(TaskQueues.ORCHESTRATION)
                    .setWorkflowTaskTimeout(Failover.workflowTaskTimeout())
                    .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                    .setTypedSearchAttributes(SearchAttrs.forLock(lockKey))
                    .build());
            try {
                WorkflowClient.startUpdateWithStart(
                        lock::acquire,
                        request,
                        UpdateOptions.<Void>newBuilder()
                                .setUpdateId(request.requestId() + "@" + lockKey)
                                .setWaitForStage(WorkflowUpdateStage.ACCEPTED)
                                .build(),
                        new WithStartWorkflowOperation<>(lock::run, LockState.fresh(lockKey, LOCK_IDLE_SECONDS)));
                break;
            } catch (WorkflowUpdateException e) {
                if (String.valueOf(e.getCause() == null ? null : e.getCause().getMessage()).contains(KeyLockWorkflowImpl.DUPLICATE)) {
                    return; // already queued or already done
                }
                retryOrThrow(attempt, e);
            } catch (WorkflowException e) {
                // e.g. the lock run completed (idle) while the update was in flight: a retry starts a new run.
                retryOrThrow(attempt, e);
            }
        }
        traces.add(new TraceRow(request.requestId(), System.currentTimeMillis(), TraceRow.Stage.LOCK_WAIT,
                request.event().entityKey(), lockKey, "position " + (request.keyIndex() + 1) + "/" + request.keys().size()));
    }

    private static void retryOrThrow(int attempt, RuntimeException e) {
        if (attempt >= 5) {
            throw e;
        }
        try {
            Thread.sleep(20L * attempt);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    @Override
    public void releaseLocks(List<String> lockKeys, String requestId) {
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (String key : lockKeys) {
                pool.submit(() -> {
                    try {
                        client.newWorkflowStub(KeyLockWorkflow.class, WorkflowIds.lock(key)).release(requestId);
                        traces.add(new TraceRow(requestId, System.currentTimeMillis(), TraceRow.Stage.RELEASED, null, key, null));
                    } catch (RuntimeException e) {
                        log.warn("release of {} on {} failed: {}", requestId, key, e.getMessage());
                    }
                });
            }
        }
    }

    @Override
    public void recordTrace(List<TraceRow> rows) {
        traces.addAll(rows);
    }

    private void traceDone(EventEnvelope event, TransitionResult r) {
        long now = System.currentTimeMillis();
        long e2e = event.ingestTsMillis() > 0 ? now - event.ingestTsMillis() : -1;
        long fromSource = event.sourceTsMillis() > 0 ? now - event.sourceTsMillis() : -1;
        // e2eMs: Kinesis read -> transition done. srcMs: producer timestamp -> transition done.
        traces.add(new TraceRow(event.eventId(), now, TraceRow.Stage.DONE, event.entityKey(), null,
                (r.accepted() ? "accepted" : "rejected") + " e2eMs=" + e2e + " srcMs=" + fromSource));
    }
}
