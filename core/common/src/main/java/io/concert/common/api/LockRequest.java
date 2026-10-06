package io.concert.common.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.concert.common.EventEnvelope;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An event travelling through its lock chain.
 *
 * @param requestId the event id (an operator retry of a parked event-style event uses {@code <eventId>~r<n>})
 * @param keyIndex position of the lock this request is queued on, in {@code event.effectiveLockKeys()}
 * @param enqueuedAtMillis when it was enqueued on the current lock
 * @param waitedMs time spent waiting on each lock already held
 */
public record LockRequest(
        String requestId, EventEnvelope event, int keyIndex, long enqueuedAtMillis, Map<String, Long> waitedMs) {

    public LockRequest {
        waitedMs = waitedMs == null ? Map.of() : Map.copyOf(waitedMs);
    }

    public static LockRequest first(EventEnvelope e, long now) {
        return new LockRequest(e.eventId(), e, 0, now, Map.of());
    }

    /** A second pass of an already-seen event through its chain (operator retry), under a new request id. */
    public static LockRequest retry(EventEnvelope e, String requestId, long now) {
        return new LockRequest(requestId, e, 0, now, Map.of());
    }

    @JsonIgnore
    public List<String> keys() {
        return event.effectiveLockKeys();
    }

    @JsonIgnore
    public String key() {
        return keys().get(keyIndex);
    }

    @JsonIgnore
    public boolean isLastKey() {
        return keyIndex == keys().size() - 1;
    }

    /** Waits so far including the current key, in acquisition order. */
    public Map<String, Long> waitsIncludingCurrent(long now) {
        Map<String, Long> w = new LinkedHashMap<>();
        for (String k : keys().subList(0, keyIndex)) {
            w.put(k, waitedMs.getOrDefault(k, 0L));
        }
        w.put(key(), Math.max(0, now - enqueuedAtMillis));
        return w;
    }

    public LockRequest forwardedTo(int nextIndex, long now) {
        return new LockRequest(requestId, event, nextIndex, now, waitsIncludingCurrent(now));
    }
}
