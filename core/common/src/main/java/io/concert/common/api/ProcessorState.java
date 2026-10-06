package io.concert.common.api;

import io.concert.common.EventEnvelope;
import java.util.List;

/**
 * Input of an event processor run ({@code evproc:<domain>:<key>}); carried across continue-as-new.
 *
 * @param scheduled future-dated events waiting for their timer (hold no locks)
 * @param blocked events in ERROR_BLOCKING; their lock chain is held until retried or skipped
 * @param parked events in ERROR_NON_BLOCKING; their keys are released
 * @param recent latest outcomes, for the status query
 * @param recentDone bounded window of event ids that finished (DONE) in this processor
 * @param lastVersion last lifecycle-row version written (row versions are monotonic per processor)
 */
public record ProcessorState(
        String domain,
        String key,
        int idleSeconds,
        List<EventEnvelope> scheduled,
        List<Pending> blocked,
        List<Pending> parked,
        List<ProcessorStatus.Recent> recent,
        List<String> recentDone,
        long lastVersion) {

    /**
     * A blocked or parked event.
     *
     * @param requestId blocked: the lock request holding the keys; parked: the retry request in flight
     *     ({@code null} when none)
     * @param lockKey blocked: the lock to unblock (the event's last key)
     * @param attempts handler attempts so far, over all runs
     * @param retries operator retries so far
     */
    public record Pending(
            EventEnvelope event, String requestId, String lockKey, String error, int attempts, int retries, long sinceMillis) {}

    public ProcessorState {
        scheduled = scheduled == null ? List.of() : List.copyOf(scheduled);
        blocked = blocked == null ? List.of() : List.copyOf(blocked);
        parked = parked == null ? List.of() : List.copyOf(parked);
        recent = recent == null ? List.of() : List.copyOf(recent);
        recentDone = recentDone == null ? List.of() : List.copyOf(recentDone);
    }

    public static ProcessorState fresh(String domain, String key, int idleSeconds) {
        return new ProcessorState(domain, key, idleSeconds, List.of(), List.of(), List.of(), List.of(), List.of(), 0);
    }
}
