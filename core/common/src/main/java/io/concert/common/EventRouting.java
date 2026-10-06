package io.concert.common;

import io.concert.common.api.EventOutcome;
import io.concert.common.api.EventProcessorWorkflow;
import io.concert.common.api.KeyLockWorkflow;
import io.concert.common.api.LockRequest;
import io.concert.common.api.LockState;
import io.concert.common.api.ProcessRequest;
import io.concert.common.api.ProcessorState;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.client.UpdateOptions;
import io.temporal.client.WithStartWorkflowOperation;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowUpdateException;
import io.temporal.client.WorkflowUpdateStage;
import java.util.function.Supplier;

/**
 * Client-side calls that put events into the lock chain or into a processor, shared by the coordinator
 * (ingest, lock workflows' local activities) and workers (events emitted by handlers and entity transitions).
 * Every call is idempotent: update ids are derived from request / event ids.
 */
public final class EventRouting {
    private EventRouting() {}

    public static final int LOCK_IDLE_SECONDS = Env.getInt("LOCK_IDLE_SECONDS", 600);
    public static final int PROCESSOR_IDLE_SECONDS = Env.getInt("EVENT_PROCESSOR_IDLE_SECONDS", 600);

    /**
     * Puts an event where it belongs, given the current time: a future-dated event-style event into its
     * processor's schedule (no locks), anything else at the end of the lock of its first sorted key.
     */
    public static void submit(WorkflowClient client, EventEnvelope e, long nowMillis) {
        if (e.isEventStyle() && e.scheduledAtMillis() > nowMillis) {
            schedule(client, e);
        } else {
            enqueueLock(client, e.effectiveLockKeys().get(0), LockRequest.first(e, nowMillis));
        }
    }

    /**
     * UpdateWithStart {@code acquire} on {@code lock:<key>}, returning once the request is accepted into the
     * queue (that fixes its order). Duplicates are treated as success. Retries a few times when the lock run
     * completes (idle) while the update is in flight.
     *
     * @return false if the request was a duplicate (already queued or recently done)
     */
    public static boolean enqueueLock(WorkflowClient client, String lockKey, LockRequest request) {
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
                return true;
            } catch (WorkflowUpdateException e) {
                if (String.valueOf(e.getCause() == null ? null : e.getCause().getMessage()).contains(KeyLockWorkflow.DUPLICATE)) {
                    return false; // already queued or already done
                }
                retryOrThrow(attempt, e);
            } catch (WorkflowException e) {
                // e.g. the lock run completed (idle) while the update was in flight: a retry starts a new run.
                retryOrThrow(attempt, e);
            }
        }
    }

    /** UpdateWithStart {@code schedule} on the event's processor; returns once accepted. */
    public static void schedule(WorkflowClient client, EventEnvelope e) {
        withRetry(() -> {
            EventProcessorWorkflow p = processorStub(client, e.domain(), e.processorKey());
            return WorkflowClient.startUpdateWithStart(
                    p::schedule,
                    e,
                    UpdateOptions.<EventOutcome>newBuilder()
                            .setUpdateId("schedule:" + e.eventId())
                            .setWaitForStage(WorkflowUpdateStage.ACCEPTED)
                            .build(),
                    new WithStartWorkflowOperation<>(p::run, freshProcessor(e)));
        });
    }

    /** UpdateWithStart {@code process} on the event's processor, waiting for the outcome. */
    public static EventOutcome process(WorkflowClient client, ProcessRequest request) {
        EventEnvelope e = request.event();
        return withRetry(() -> {
            EventProcessorWorkflow p = processorStub(client, e.domain(), e.processorKey());
            return WorkflowClient.executeUpdateWithStart(
                    p::process,
                    request,
                    UpdateOptions.<EventOutcome>newBuilder()
                            .setUpdateId("process:" + request.requestId())
                            .setWaitForStage(WorkflowUpdateStage.COMPLETED)
                            .build(),
                    new WithStartWorkflowOperation<>(p::run, freshProcessor(e)));
        });
    }

    /** Sends {@code unblock} to a lock (it is running: a blocked head keeps it open). */
    public static void unblock(WorkflowClient client, String lockKey, String requestId) {
        client.newWorkflowStub(KeyLockWorkflow.class, WorkflowIds.lock(lockKey)).unblock(requestId);
    }

    /** A stub for an existing processor (operator calls and queries). */
    public static EventProcessorWorkflow processor(WorkflowClient client, String domain, String key) {
        return client.newWorkflowStub(EventProcessorWorkflow.class, WorkflowIds.processor(domain, key));
    }

    private static EventProcessorWorkflow processorStub(WorkflowClient client, String domain, String key) {
        return client.newWorkflowStub(EventProcessorWorkflow.class, WorkflowOptions.newBuilder()
                .setWorkflowId(WorkflowIds.processor(domain, key))
                .setTaskQueue(TaskQueues.events(domain))
                .setWorkflowTaskTimeout(Failover.workflowTaskTimeout())
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                .setTypedSearchAttributes(SearchAttrs.forEntity(domain, key))
                .build());
    }

    private static ProcessorState freshProcessor(EventEnvelope e) {
        return ProcessorState.fresh(e.domain(), e.processorKey(), PROCESSOR_IDLE_SECONDS);
    }

    private static <T> T withRetry(Supplier<T> call) {
        for (int attempt = 1; ; attempt++) {
            try {
                return call.get();
            } catch (WorkflowUpdateException e) {
                throw e; // rejected by the validator or failed in the handler: deterministic
            } catch (WorkflowException e) {
                // the processor run completed (idle) while the update was in flight: a retry starts a new run
                retryOrThrow(attempt, e);
            }
        }
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
}
