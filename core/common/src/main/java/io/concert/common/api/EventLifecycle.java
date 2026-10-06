package io.concert.common.api;

/**
 * The lifecycle every event-style event goes through:
 *
 * <pre>
 * NEW --(locks acquired, handler ok)--> DONE
 *  |                  +-- NonBlockingError / exhausted (onError NON_BLOCKING) --> ERROR_NON_BLOCKING --(retry ok | skip)--> DONE
 *  |                  +-- BlockingError / exhausted (default)                 --> ERROR_BLOCKING ----(retry ok | skip)--> DONE
 *  +--(scheduledAt &gt; now)--> SCHEDULED --(timer)--> NEW ...
 * </pre>
 *
 * ERROR_BLOCKING keeps the event's lock keys held (later events on them wait); ERROR_NON_BLOCKING parks the
 * event and releases them.
 */
public enum EventLifecycle {
    NEW,
    SCHEDULED,
    DONE,
    ERROR_BLOCKING,
    ERROR_NON_BLOCKING
}
