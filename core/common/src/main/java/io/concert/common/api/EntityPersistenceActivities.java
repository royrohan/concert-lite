package io.concert.common.api;

import io.concert.common.TraceRow;
import io.temporal.activity.ActivityInterface;

/**
 * Local activities of entity workflows against the state store, which holds the entities (DB-truth): a transition is
 * one database transaction (state row, applied-event record, outbox of emitted events), not a projection written
 * after the fact.
 */
@ActivityInterface
public interface EntityPersistenceActivities {

    /** The entity as committed and, if {@code eventId} was applied before, its stored result. */
    EntityLoad loadEntity(String workflowId, String eventId);

    /**
     * Commits a transition atomically (see {@code StateStore.commitTransition}). {@code trace} (may be null) is
     * recorded, best effort, when the transition commits.
     */
    CommitOutcome commitTransition(TransitionCommit commit, TraceRow trace);
}
