package io.concert.common.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;

/**
 * What the processor did with an event-style event; returned by {@code process}, {@code retry} and
 * {@code skip}.
 *
 * @param status DONE, ERROR_BLOCKING (the lock keeps holding the event's keys), ERROR_NON_BLOCKING (parked,
 *     keys released) or SCHEDULED
 * @param detail error message, skip reason or note ({@code null} when DONE normally)
 * @param attempts handler attempts of the last run (0 when the handler did not run)
 * @param children ids of the events it emitted (DONE only)
 */
public record EventOutcome(String eventId, EventLifecycle status, String detail, int attempts, List<String> children) {

    public EventOutcome {
        children = children == null ? List.of() : List.copyOf(children);
    }

    /** The lock must keep holding the event's keys until the processor sends {@code unblock}. */
    @JsonIgnore
    public boolean blocked() {
        return status == EventLifecycle.ERROR_BLOCKING;
    }
}
