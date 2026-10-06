package io.concert.common.api;

import io.concert.common.EventEnvelope;
import io.concert.common.TraceRow;
import io.temporal.activity.ActivityInterface;
import java.util.List;
import java.util.Map;

/** Called as local activities from lock and event workflows. */
@ActivityInterface
public interface DispatchActivities {

    /**
     * UpdateWithStart on the entity workflow, waiting for the transition to complete. Also records
     * LOCK_GRANTED / DONE trace rows asynchronously, so tracing costs the workflows nothing.
     *
     * @param lockWaitMs how long the caller waited for each lock key
     */
    TransitionResult dispatchToEntity(EventEnvelope event, Map<String, Long> lockWaitMs);

    /**
     * Event style: UpdateWithStart {@code process} on the event's processor ({@code evproc:<domain>:<firstKey>}),
     * waiting for the outcome. Records LOCK_GRANTED trace rows like {@link #dispatchToEntity}.
     */
    EventOutcome dispatchToProcessor(ProcessRequest request);

    /** Event style, future-dated: UpdateWithStart {@code schedule} on the event's processor (no locks taken). */
    void scheduleEvent(EventEnvelope event);

    /**
     * UpdateWithStart {@code acquire} on {@code lock:<key>}, returning once the request is accepted
     * into the queue (that fixes its order). Duplicates are treated as success.
     */
    void enqueueLock(String lockKey, LockRequest request);

    /** Sends {@code release} to each lock in parallel. */
    void releaseLocks(List<String> lockKeys, String requestId);

    void recordTrace(List<TraceRow> rows);
}
