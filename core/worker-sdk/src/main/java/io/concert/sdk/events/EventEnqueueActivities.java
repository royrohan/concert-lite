package io.concert.sdk.events;

import io.temporal.activity.ActivityInterface;

/**
 * Regular (not local) activities that call into other workflows on behalf of processors and entity machines.
 * They must not be local activities: the caller is typically in the middle of an update that a lock is
 * waiting for, and a local activity would hold the caller's workflow task open while the target lock may be
 * waiting on the caller (see the deadlock note in {@code KeyLockWorkflowImpl}). Scheduling the activity is
 * recorded in history atomically with the caller's outcome, which makes the hand-off durable.
 */
@ActivityInterface(namePrefix = "EventEmit")
public interface EventEnqueueActivities {

    /** Enqueues lock requests (in order) and schedules future-dated events; duplicates are no-ops. */
    void enqueue(EnqueueRequest request);

    /** Sends {@code unblock(requestId)} to {@code lock:<lockKey>}. */
    void unblock(String lockKey, String requestId);
}
