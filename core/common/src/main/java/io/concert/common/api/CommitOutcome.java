package io.concert.common.api;

/**
 * What the store did with a {@link TransitionCommit}.
 *
 * @param result COMMITTED: the committed result; ALREADY_APPLIED: the result stored when the event was first applied
 * @param current CONFLICT: the entity as it is now (null if it does not exist), to decide again on
 */
public record CommitOutcome(Status status, TransitionResult result, SmStateRow current) {

    public enum Status {
        /** State, applied event and outbox were written together. */
        COMMITTED,
        /** The event was applied before (a redelivery): nothing was written. */
        ALREADY_APPLIED,
        /** The entity is not at the expected version: nothing was written; decide again on {@code current}. */
        CONFLICT
    }

    public static CommitOutcome committed(TransitionResult r) {
        return new CommitOutcome(Status.COMMITTED, r, null);
    }

    public static CommitOutcome alreadyApplied(TransitionResult stored) {
        return new CommitOutcome(Status.ALREADY_APPLIED, stored, null);
    }

    public static CommitOutcome conflict(SmStateRow current) {
        return new CommitOutcome(Status.CONFLICT, null, current);
    }
}
