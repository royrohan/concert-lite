package io.concert.sdk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** smType -> spec, so tools like the trace UI can draw diagrams without running a worker. */
public final class StateMachineRegistry {
    private StateMachineRegistry() {}

    private static final Map<String, StateMachineSpec> SPECS = new ConcurrentHashMap<>();

    public static void register(String smType, StateMachineSpec spec) {
        SPECS.put(smType, spec);
    }

    public static StateMachineSpec get(String smType) {
        return SPECS.get(smType);
    }

    public static Map<String, StateMachineSpec> all() {
        return Map.copyOf(SPECS);
    }
}
