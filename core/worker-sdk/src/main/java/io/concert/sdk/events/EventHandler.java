package io.concert.sdk.events;

/**
 * Applies one event type. Runs in a local activity of the event's processor with all of the event's lock
 * keys held, so it may do I/O; it is retried per the type's policy and must therefore tolerate re-runs (its
 * {@link EventContext} writes and emits are only applied once, after it returns).
 *
 * <p>Declare the event class with {@link Handles} or let it be inferred from the type argument. Throw
 * {@link BlockingError} / {@link NonBlockingError} to fail without retrying; any other exception is retried and,
 * once attempts are exhausted, handled per {@link EventCatalog.EventType#onError()} (default BLOCKING).
 *
 * @param <T> the event (payload) class, bound from the event's JSON payload
 */
@FunctionalInterface
public interface EventHandler<T> {

    void apply(T event, EventContext ctx) throws Exception;
}
