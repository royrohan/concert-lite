package io.concert.sdk.events;

import io.concert.common.EventEnvelope;
import java.util.List;

/**
 * Result of one handler run (the local activity), applied by the processor.
 *
 * @param attempt the successful (or classifying) attempt number
 * @param writes keyed-state documents to write (OK only)
 * @param emitted child events with their final ids, in emission order (OK; for ALREADY_DONE the ones recorded
 *     in the lifecycle row, re-enqueued idempotently)
 * @param storedData ALREADY_DONE: the stored lifecycle row data
 * @param storedVersion ALREADY_DONE: its version
 */
public record HandlerResult(
        Status status, String error, int attempt, List<StateWrite> writes, List<EventEnvelope> emitted,
        String storedData, long storedVersion) {

    public enum Status {
        /** Applied: persist writes, go DONE, enqueue children. */
        OK,
        /** {@link BlockingError}: ERROR_BLOCKING. */
        BLOCKING,
        /** {@link NonBlockingError} or an unbindable payload: ERROR_NON_BLOCKING. */
        NON_BLOCKING,
        /** No handler for the event type in this domain: ERROR_NON_BLOCKING. */
        NO_HANDLER,
        /** The lifecycle row already says DONE (a duplicate delivery, or a re-run after a crash). */
        ALREADY_DONE
    }

    public HandlerResult {
        writes = writes == null ? List.of() : List.copyOf(writes);
        emitted = emitted == null ? List.of() : List.copyOf(emitted);
    }

    public static HandlerResult ok(int attempt, List<StateWrite> writes, List<EventEnvelope> emitted) {
        return new HandlerResult(Status.OK, null, attempt, writes, emitted, null, 0);
    }

    public static HandlerResult failed(Status status, String error, int attempt) {
        return new HandlerResult(status, error, attempt, List.of(), List.of(), null, 0);
    }
}
