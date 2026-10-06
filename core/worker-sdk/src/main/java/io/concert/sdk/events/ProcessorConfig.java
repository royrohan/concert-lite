package io.concert.sdk.events;

import io.concert.common.Env;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-event-type handler policy of a processor: attempts (local activity retries), backoff and what happens
 * on exhaustion. Built from the domain's {@link EventCatalog} entries; workflow code reads it, so it must be
 * the same on every worker of the domain (like the workflow code itself).
 */
public final class ProcessorConfig {

    /**
     * @param maxAttempts handler attempts before {@code onError} applies
     * @param initialInterval first retry backoff (doubles up to {@code maxInterval})
     * @param startToClose one attempt's timeout
     */
    public record Policy(OnError onError, int maxAttempts, Duration initialInterval, Duration maxInterval,
            Duration startToClose) {

        public Policy withOnError(OnError o) {
            return new Policy(o, maxAttempts, initialInterval, maxInterval, startToClose);
        }

        public Policy withMaxAttempts(int n) {
            return new Policy(onError, Math.max(1, n), initialInterval, maxInterval, startToClose);
        }
    }

    public static final int DEFAULT_CONTINUE_AFTER = 500;

    private final Policy defaults;
    private final Map<String, Policy> byType;
    private final int continueAfter;

    public ProcessorConfig(Policy defaults, Map<String, Policy> byType) {
        this(defaults, byType, DEFAULT_CONTINUE_AFTER);
    }

    public ProcessorConfig(Policy defaults, Map<String, Policy> byType, int continueAfter) {
        this.defaults = defaults;
        this.byType = Map.copyOf(byType);
        this.continueAfter = continueAfter;
    }

    public ProcessorConfig withContinueAfter(int n) {
        return new ProcessorConfig(defaults, byType, n);
    }

    /** Events applied per run before the processor continues as new. */
    public int continueAfter() {
        return continueAfter;
    }

    /** BLOCKING, {@code EVENT_HANDLER_MAX_ATTEMPTS} (3) attempts, 200 ms .. 5 s backoff, 30 s per attempt. */
    public static Policy defaultPolicy() {
        return new Policy(OnError.BLOCKING, Env.getInt("EVENT_HANDLER_MAX_ATTEMPTS", EventCatalog.EventType.DEFAULT_ATTEMPTS),
                Duration.ofMillis(Env.getLong("EVENT_HANDLER_RETRY_INITIAL_MS", 200)), Duration.ofSeconds(5),
                Duration.ofSeconds(30));
    }

    /** Policies of the domain's catalog entries over {@code defaults}. */
    public static ProcessorConfig forDomain(String domain, Policy defaults) {
        return of(defaults, EventCatalogs.forDomain(domain));
    }

    public static ProcessorConfig of(Policy defaults, List<EventCatalog.EventType> types) {
        Map<String, Policy> m = new HashMap<>();
        for (EventCatalog.EventType t : types) {
            m.put(t.name(), defaults.withOnError(t.onError()).withMaxAttempts(t.maxAttempts()));
        }
        return new ProcessorConfig(defaults, m);
    }

    public Policy policy(String eventType) {
        return byType.getOrDefault(eventType, defaults);
    }
}
