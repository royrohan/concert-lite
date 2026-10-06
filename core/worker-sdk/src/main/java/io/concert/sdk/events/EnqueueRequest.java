package io.concert.sdk.events;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.concert.common.EventEnvelope;
import io.concert.common.api.LockRequest;
import java.util.ArrayList;
import java.util.List;

/**
 * Events to put into Temporal, routed by the emitter (in workflow code, with workflow time): due events onto
 * the lock of their first key, future-dated event-style events into their processor's schedule.
 *
 * @param locks lock requests, enqueued in order on {@code request.key()}
 * @param schedules future-dated events
 * @param emittedBy parent event id for EMITTED trace rows ({@code null}: not emitted, e.g. a fired timer)
 */
public record EnqueueRequest(List<LockRequest> locks, List<EventEnvelope> schedules, String emittedBy) {

    public EnqueueRequest {
        locks = locks == null ? List.of() : List.copyOf(locks);
        schedules = schedules == null ? List.of() : List.copyOf(schedules);
    }

    /** Routes emitted children by {@code nowMillis} (workflow time). */
    public static EnqueueRequest children(String parentId, List<EventEnvelope> children, long nowMillis) {
        List<LockRequest> locks = new ArrayList<>();
        List<EventEnvelope> schedules = new ArrayList<>();
        for (EventEnvelope c : children) {
            if (c.isEventStyle() && c.scheduledAtMillis() > nowMillis) {
                schedules.add(c);
            } else {
                locks.add(LockRequest.first(c, nowMillis));
            }
        }
        return new EnqueueRequest(locks, schedules, parentId);
    }

    public static EnqueueRequest lock(LockRequest request) {
        return new EnqueueRequest(List.of(request), List.of(), null);
    }

    @JsonIgnore
    public boolean isEmpty() {
        return locks.isEmpty() && schedules.isEmpty();
    }
}
