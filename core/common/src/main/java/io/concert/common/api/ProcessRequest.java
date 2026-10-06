package io.concert.common.api;

import io.concert.common.EventEnvelope;
import java.util.Map;

/**
 * Sent by the last lock of an event-style chain to the processor: apply the event now (all its keys are held).
 *
 * @param requestId the lock request id: the event id, or {@code <eventId>~r<n>} for an operator retry of a
 *     parked event; a blocked outcome is released with {@code unblock(requestId)} on {@code lockKey}
 * @param lockKey the lock that dispatched (the event's last key)
 * @param lockWaitMs how long the request waited for each key (for traces)
 */
public record ProcessRequest(String requestId, EventEnvelope event, String lockKey, Map<String, Long> lockWaitMs) {

    public ProcessRequest {
        lockWaitMs = lockWaitMs == null ? Map.of() : Map.copyOf(lockWaitMs);
    }
}
