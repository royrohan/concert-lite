package io.concert.common.api;

import io.concert.common.EventEnvelope;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Map;

/**
 * Applies one event to its entity: called by the last lock of the event's chain as a regular activity on the task
 * queue {@code sm-<smType>}, where the type's worker runs it. The entity lives in the state store; the activity loads
 * it, runs the machine's decision and commits state, applied-event record and outbox in one database transaction.
 * Retries are idempotent: an event applied before returns its stored result.
 */
@ActivityInterface
public interface TransitionActivities {

    /**
     * @param lockWaitMs how long the event waited for each of its lock keys (traced as LOCK_GRANTED)
     * @return the transition's result; a rejected transition is a result, not a failure
     */
    @ActivityMethod(name = "ApplyTransition")
    TransitionResult applyTransition(EventEnvelope event, Map<String, Long> lockWaitMs);
}
