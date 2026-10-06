package io.concert.sdk.events;

import io.concert.common.EventEnvelope;
import io.concert.common.TraceRow;
import io.concert.common.api.SmStateRow;
import io.temporal.activity.ActivityInterface;
import java.util.List;

/** Local activities of the event processor (on its {@code ev-<domain>} worker). */
@ActivityInterface(namePrefix = "EventHandler")
public interface EventHandlerActivities {

    /**
     * Runs the handler for the event's type. Writes nothing: state writes and emitted events come back in the
     * result. {@link BlockingError} / {@link NonBlockingError} / a missing handler / an unbindable payload are
     * classified in the result; other exceptions fail the attempt (retried per the type's policy).
     */
    HandlerResult apply(EventEnvelope event);

    /**
     * Writes rows in order (state documents first, the lifecycle row last, all version-guarded so a repeat is
     * harmless) and records trace rows.
     */
    void persist(List<SmStateRow> rows, List<TraceRow> traces);
}
