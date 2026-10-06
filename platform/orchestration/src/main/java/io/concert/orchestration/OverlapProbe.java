package io.concert.orchestration;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies the lock guarantee where it matters: while an event is being applied to its entity, no
 * other event holding any of the same keys may be in flight. Cheap enough to leave on; tests read
 * {@link #violations()}. Only meaningful within one orchestration process.
 */
public final class OverlapProbe {
    private OverlapProbe() {}

    private static final Set<String> HELD = ConcurrentHashMap.newKeySet();
    private static final AtomicLong VIOLATIONS = new AtomicLong();

    static void enter(List<String> keys) {
        for (String k : keys) {
            if (!HELD.add(k)) {
                VIOLATIONS.incrementAndGet();
            }
        }
    }

    static void exit(List<String> keys) {
        keys.forEach(HELD::remove);
    }

    public static long violations() {
        return VIOLATIONS.get();
    }

    public static void reset() {
        HELD.clear();
        VIOLATIONS.set(0);
    }
}
