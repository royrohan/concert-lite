package io.concert.common.api;

import io.concert.common.EventEnvelope;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Applies event-style events of one domain whose first lock key is {@code key}: workflow id
 * {@code evproc:<domain>:<key>} on task queue {@code ev-<domain>}, started on demand with UpdateWithStart and
 * kept warm (idle completion), like an entity workflow. The implementation lives in the worker SDK
 * ({@code io.concert.sdk.events.EventProcessorWorkflowImpl}); the interface is here so the coordinator can call it.
 */
@WorkflowInterface
public interface EventProcessorWorkflow {

    @WorkflowMethod
    void run(ProcessorState state);

    /**
     * Applies the event; the caller (the last lock of its chain) holds all of its keys. Returns once the
     * outcome is persisted: DONE / ERROR_NON_BLOCKING (the caller releases the keys) or ERROR_BLOCKING (the
     * caller keeps holding them until {@code unblock}).
     */
    @UpdateMethod
    EventOutcome process(ProcessRequest request);

    @UpdateValidatorMethod(updateName = "process")
    void validateProcess(ProcessRequest request);

    /** Keeps a future-dated event until its {@code scheduledAtMillis}, then enqueues it on its lock chain. */
    @UpdateMethod
    EventOutcome schedule(EventEnvelope event);

    @UpdateValidatorMethod(updateName = "schedule")
    void validateSchedule(EventEnvelope event);

    /**
     * Operator retry of a blocked event (the handler runs again right away, its keys are still held) or of a
     * parked one (it re-enters its lock chain with a new request id; the returned status stays
     * ERROR_NON_BLOCKING until it has run).
     */
    @UpdateMethod
    EventOutcome retry(String eventId);

    @UpdateValidatorMethod(updateName = "retry")
    void validateRetry(String eventId);

    /** Operator skip of a blocked or parked event: it goes DONE without running; a blocked one frees its keys. */
    @UpdateMethod
    EventOutcome skip(String eventId, String reason);

    @UpdateValidatorMethod(updateName = "skip")
    void validateSkip(String eventId, String reason);

    @QueryMethod
    ProcessorStatus status();
}
