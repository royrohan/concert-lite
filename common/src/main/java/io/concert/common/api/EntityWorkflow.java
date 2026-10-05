package io.concert.common.api;

import io.concert.common.EventEnvelope;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * One state machine instance, workflow id {@code smType:instanceKey}, task queue {@code sm-<type>}.
 * Every type implements this same interface (see worker-sdk {@code AbstractStateMachine}); the
 * task queue decides which implementation runs.
 */
@WorkflowInterface
public interface EntityWorkflow {

    @WorkflowMethod
    void run(EntityInit init);

    /** Applies one event. Callers use updateId = eventId, so retries are idempotent per run. */
    @UpdateMethod
    TransitionResult handle(EventEnvelope event);

    @UpdateValidatorMethod(updateName = "handle")
    void validateHandle(EventEnvelope event);

    @QueryMethod
    EntitySnapshot snapshot();
}
