package io.concert.common.api;

import java.util.List;

/**
 * Query result of an event processor.
 *
 * @param scheduled waiting future-dated events
 * @param blocked events in ERROR_BLOCKING (holding their keys)
 * @param parked events in ERROR_NON_BLOCKING ({@code requestId} set while an operator retry is in flight)
 * @param recent latest outcomes, oldest first
 */
public record ProcessorStatus(
        String domain,
        String key,
        List<Item> scheduled,
        List<Item> blocked,
        List<Item> parked,
        List<Recent> recent,
        long processedThisRun) {

    /** One waiting / blocked / parked event. */
    public record Item(String eventId, String eventType, long atMillis, String requestId, String error, int attempts) {}

    /** One outcome. */
    public record Recent(String eventId, String eventType, EventLifecycle status, String detail, long tsMillis) {}
}
