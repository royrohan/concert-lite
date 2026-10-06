package io.concert.sdk.events;

import io.concert.common.EventEnvelope;
import io.concert.common.EventRouting;
import io.concert.common.TraceRow;
import io.concert.common.api.LockRequest;
import io.concert.store.TraceWriter;
import io.temporal.client.WorkflowClient;

/** Client-side implementation of {@link EventEnqueueActivities}. */
public final class EventEnqueueActivitiesImpl implements EventEnqueueActivities {

    private final WorkflowClient client;
    private final TraceWriter traces;

    /** @param traces EMITTED / LOCK_WAIT trace rows go here ({@code null}: not traced) */
    public EventEnqueueActivitiesImpl(WorkflowClient client, TraceWriter traces) {
        this.client = client;
        this.traces = traces;
    }

    @Override
    public void enqueue(EnqueueRequest request) {
        long now = System.currentTimeMillis();
        for (LockRequest r : request.locks()) {
            boolean queued = EventRouting.enqueueLock(client, r.key(), r);
            trace(r.event(), now, request.emittedBy(), queued ? r.key() : null);
        }
        for (EventEnvelope e : request.schedules()) {
            EventRouting.schedule(client, e);
            trace(e, now, request.emittedBy(), null);
        }
    }

    @Override
    public void unblock(String lockKey, String requestId) {
        EventRouting.unblock(client, lockKey, requestId);
    }

    private void trace(EventEnvelope e, long now, String parent, String lockKey) {
        if (traces == null) {
            return;
        }
        if (parent != null) {
            traces.add(new TraceRow(e.eventId(), now, TraceRow.Stage.EMITTED, e.traceWorkflowId(), null,
                    "parent=" + parent + " root=" + e.causationRoot() + " type=" + e.eventType()
                            + (e.scheduledAtMillis() > 0 ? " scheduledAt=" + e.scheduledAtMillis() : "")));
        }
        if (lockKey != null) {
            traces.add(new TraceRow(e.eventId(), now, TraceRow.Stage.LOCK_WAIT, e.traceWorkflowId(), lockKey,
                    "position 1/" + e.effectiveLockKeys().size()));
        }
    }
}
