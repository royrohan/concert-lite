package io.concert.sdk;

import java.util.List;
import java.util.ServiceLoader;

/**
 * Activities of your own that a machine calls from {@code onTransition}: REST clients, Kafka producers, ...
 * An ecosystem provides them through {@link ServiceLoader} (a {@code META-INF/services/io.concert.sdk.WorkerActivities}
 * file naming the implementation). The generated worker registers {@link #create} on each machine's task queue
 * ({@code sm-<smType>}, where the machine's activity stubs run by default), and the generated test registers
 * {@link #forTests}, so happy paths can run without the real services.
 *
 * <p>Activities are retried by Temporal until they succeed, time out or fail with a non-retryable error, and a completed
 * activity is never run again on replay: side effects happen at least once, so give every write an idempotency key the
 * receiver deduplicates on (for example derived from the instance key or the event id, which are stable across retries).
 */
public interface WorkerActivities {

    /** Activity implementations for the worker of {@code smType}; empty when the machine calls none. */
    List<Object> create(String smType);

    /** Implementations for the generated tests (fakes, no network). Default: none. */
    default List<Object> forTests(String smType) {
        return List.of();
    }

    /** Every provider's {@link #create} for {@code smType}. */
    static List<Object> load(String smType) {
        return ServiceLoader.load(WorkerActivities.class).stream().flatMap(p -> p.get().create(smType).stream()).toList();
    }

    /** Every provider's {@link #forTests} for {@code smType}. */
    static List<Object> loadForTests(String smType) {
        return ServiceLoader.load(WorkerActivities.class).stream().flatMap(p -> p.get().forTests(smType).stream()).toList();
    }
}
