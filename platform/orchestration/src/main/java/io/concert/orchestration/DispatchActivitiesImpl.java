package io.concert.orchestration;

import io.concert.common.EventEnvelope;
import io.concert.common.EventRouting;
import io.concert.common.TraceRow;
import io.concert.common.WorkflowIds;
import io.concert.common.api.DispatchActivities;
import io.concert.common.api.EventOutcome;
import io.concert.common.api.KeyLockWorkflow;
import io.concert.common.api.LockRequest;
import io.concert.common.api.ProcessRequest;
import io.concert.store.TraceWriter;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowUpdateException;
import io.temporal.failure.ApplicationFailure;
import java.util.List;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client-side calls into Temporal on behalf of workflows (local activities) and the ingest path. */
public final class DispatchActivitiesImpl implements DispatchActivities {
    private static final Logger log = LoggerFactory.getLogger(DispatchActivitiesImpl.class);

    private final WorkflowClient client;
    private final TraceWriter traces;

    public DispatchActivitiesImpl(WorkflowClient client, TraceWriter traces) {
        this.client = client;
        this.traces = traces;
    }

    @Override
    public void enqueueLock(String lockKey, LockRequest request) {
        if (!EventRouting.enqueueLock(client, lockKey, request)) {
            return; // already queued or already done
        }
        traces.add(new TraceRow(request.requestId(), System.currentTimeMillis(), TraceRow.Stage.LOCK_WAIT,
                request.event().traceWorkflowId(), lockKey, "position " + (request.keyIndex() + 1) + "/" + request.keys().size()));
    }

    @Override
    public EventOutcome dispatchToProcessor(ProcessRequest request) {
        EventEnvelope event = request.event();
        long now = System.currentTimeMillis();
        request.lockWaitMs().forEach((key, waited) -> traces.add(new TraceRow(
                event.eventId(), now, TraceRow.Stage.LOCK_GRANTED, event.traceWorkflowId(), key, "waitedMs=" + waited)));
        List<String> keys = event.effectiveLockKeys();
        OverlapProbe.enter(keys);
        try {
            return EventRouting.process(client, request);
        } catch (WorkflowUpdateException e) {
            // Validator rejection (wrong domain / key): deterministic, retrying cannot help.
            throw ApplicationFailure.newNonRetryableFailure(
                    String.valueOf(e.getCause() != null ? e.getCause().getMessage() : e.getMessage()),
                    EntityRejectedException.TYPE);
        } finally {
            OverlapProbe.exit(keys);
        }
    }

    @Override
    public void scheduleEvent(EventEnvelope event) {
        EventRouting.schedule(client, event);
    }

    /**
     * Sends {@code release} to every lock in parallel. A release that fails (e.g. the lock's workflow task is failing
     * while its coordinator restarts) fails the activity once every key was attempted, so the caller's activity
     * retries: {@code release} is idempotent, and a lost release would leave the key held forever. A lock that no
     * longer exists has nothing to release.
     */
    @Override
    public void releaseLocks(List<String> lockKeys, String requestId) {
        List<String> failed = new java.util.concurrent.CopyOnWriteArrayList<>();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (String key : lockKeys) {
                pool.submit(() -> {
                    try {
                        client.newWorkflowStub(KeyLockWorkflow.class, WorkflowIds.lock(key)).release(requestId);
                        traces.add(new TraceRow(requestId, System.currentTimeMillis(), TraceRow.Stage.RELEASED, null, key, null));
                    } catch (io.temporal.client.WorkflowNotFoundException e) {
                        log.debug("release of {} on {}: the lock is gone, nothing to release", requestId, key);
                    } catch (RuntimeException e) {
                        log.warn("release of {} on {} failed, will retry: {}", requestId, key, e.getMessage());
                        failed.add(key);
                    }
                });
            }
        }
        if (!failed.isEmpty()) {
            throw ApplicationFailure.newFailure("release of " + requestId + " failed on " + failed, "ReleaseFailed");
        }
    }

    @Override
    public void recordTrace(List<TraceRow> rows) {
        traces.addAll(rows);
    }

}
