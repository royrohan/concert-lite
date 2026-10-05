package io.concert.common;

/**
 * {@code TRACE_SAMPLE} (0.0–1.0, default 1.0): fraction of events that get trace rows. The decision
 * is a pure function of the event id, so every process (coordinator, lock, worker) samples the same
 * events and a sampled event always has its complete timeline. 0 removes trace writes entirely.
 */
public final class TraceSampling {
    private TraceSampling() {}

    private static final int RATE_BP = (int) Math.round(
            Math.max(0, Math.min(1, Double.parseDouble(Env.get("TRACE_SAMPLE", "1.0")))) * 10_000);

    public static boolean sampled(String eventId) {
        if (RATE_BP >= 10_000) {
            return true;
        }
        if (RATE_BP <= 0 || eventId == null) {
            return false;
        }
        // String.hashCode is specified, so this is stable across JVMs.
        return Math.floorMod(eventId.hashCode() * 0x9E3779B1, 10_000) < RATE_BP;
    }
}
