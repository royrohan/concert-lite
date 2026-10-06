package io.concert.common.api;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * FIFO lock for one serialization key, workflow id {@code lock:<key>}.
 *
 * <p>An event with sorted keys K0..Kn-1 is acquired as a chain: it is enqueued on K0; when it
 * reaches the head of Ki the lock forwards it to Ki+1 and keeps holding Ki; the lock of the last key
 * dispatches the event to its entity and then releases K0..Kn-2. Keys are always taken in the same
 * global order, so the chain can never deadlock. A single-key event is simply "last" on K0.
 *
 * <p>All lock interactions are Temporal <em>updates</em>, not signals: updates reach the worker
 * directly while signals take an extra trip through the server's transfer queue (~3x slower here).
 */
@WorkflowInterface
public interface KeyLockWorkflow {

    @WorkflowMethod
    void run(LockState state);

    /** Enqueues the request; completes as soon as it is in the queue (callers wait for ACCEPTED). */
    @UpdateMethod
    void acquire(LockRequest request);

    /** Rejects duplicates (a replayed ingest or a retried forward) without touching history. */
    @UpdateValidatorMethod(updateName = "acquire")
    void validateAcquire(LockRequest request);

    /** Sent by the last lock of a chain once the event has been applied. Idempotent. */
    @UpdateMethod
    void release(String requestId);

    @QueryMethod
    LockSnapshot snapshot();
}
