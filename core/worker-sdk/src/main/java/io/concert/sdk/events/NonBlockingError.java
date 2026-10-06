package io.concert.sdk.events;

/**
 * Thrown by a handler: the event goes ERROR_NON_BLOCKING right away (no retries); it is parked and its lock
 * keys are released. An operator retry re-enters the lock chain.
 */
public class NonBlockingError extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NonBlockingError(String message) {
        super(message);
    }

    public NonBlockingError(String message, Throwable cause) {
        super(message, cause);
    }
}
