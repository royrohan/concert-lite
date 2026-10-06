package io.concert.sdk.events;

/**
 * Thrown by a handler: the event goes ERROR_BLOCKING right away (no retries) and keeps all of its lock keys,
 * so later events on any of them wait until an operator retries or skips it.
 */
public class BlockingError extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BlockingError(String message) {
        super(message);
    }

    public BlockingError(String message, Throwable cause) {
        super(message, cause);
    }
}
