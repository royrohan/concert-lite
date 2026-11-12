package io.concert.samples;

import io.concert.model.runtime.ModelObject;
import io.concert.sdk.ModelStateMachine;
import java.time.Instant;

/**
 * Base for the typed sample machines. Their payload commands carry an optional {@code workMs} that
 * triggers simulated side-effect work (used by the scaling benchmark), like the untyped
 * {@link SampleStateMachine}.
 *
 * <p>The samples accept partial or missing payloads ({@code null}, or e.g. just {@code {"workMs":40}})
 * and fill required command fields with defaults in {@link #completePayload}, so the integration tests
 * and benchmarks can send events without building full commands. The completed command is then
 * validated against its model as usual.
 */
public abstract class SampleModelStateMachine<D extends ModelObject> extends ModelStateMachine<D> {

    private final SimulatedWork work = new SimulatedWork.Impl(); // stand-in latency of a side effect

    /** Runs the simulated side effect of a command, if it asks for one. */
    protected void simulateWork(Long workMs) {
        if (workMs != null && workMs > 0) {
            work.work(workMs);
        }
    }

    /** The transition's timestamp ({@link #now()}) as an {@code Instant}. */
    protected Instant nowInstant() {
        return Instant.ofEpochMilli(now());
    }
}
