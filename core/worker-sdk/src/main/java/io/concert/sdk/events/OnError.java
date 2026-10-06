package io.concert.sdk.events;

/** What an exhausted handler (retries used up) does to its event; see {@link EventCatalog.EventType#onError()}. */
public enum OnError {
    /** ERROR_BLOCKING: the event keeps its lock keys until an operator retries or skips it (the default). */
    BLOCKING,
    /** ERROR_NON_BLOCKING: the event is parked and its keys are released; a retry re-enters the lock chain. */
    NON_BLOCKING
}
