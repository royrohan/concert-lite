package io.concert.common.api;

import io.concert.common.EventEnvelope;
import java.util.List;

/**
 * One transition of an entity, committed atomically by the state store (DB-truth): the event is recorded as applied
 * with its {@code result}, and if accepted the entity moves from {@code expectedVersion} to {@code row} and the
 * {@code outbox} events are stored for delivery. The store refuses it if the entity is no longer at
 * {@code expectedVersion} (CONFLICT) or the event was already applied (ALREADY_APPLIED).
 *
 * @param workflowId the entity, {@code smType:instanceKey}
 * @param expectedVersion the version the decision was made on (0: a new entity)
 * @param result the outcome to record; a rejected result leaves the entity unchanged
 * @param row the entity after an accepted transition ({@code version == expectedVersion + 1}); null when rejected
 * @param outbox events the transition emitted, delivered after commit; empty when rejected
 */
public record TransitionCommit(
        String workflowId, String eventId, long expectedVersion, TransitionResult result, SmStateRow row,
        List<EventEnvelope> outbox, long nowMillis) {

    public TransitionCommit {
        outbox = outbox == null ? List.of() : List.copyOf(outbox);
        if (result.accepted() && (row == null || row.version() != expectedVersion + 1)) {
            throw new IllegalArgumentException("an accepted transition needs the row at version " + (expectedVersion + 1));
        }
        if (!result.accepted() && (row != null || !outbox.isEmpty())) {
            throw new IllegalArgumentException("a rejected transition changes nothing and emits nothing");
        }
    }
}
