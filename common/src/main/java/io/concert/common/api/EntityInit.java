package io.concert.common.api;

import java.util.List;

/** Start (or continue-as-new) input of an entity workflow. {@code state == null} means fresh. */
public record EntityInit(
        String smType, String instanceKey, String state, String data, long version, List<TransitionRecord> recent) {

    public static EntityInit fresh(String smType, String instanceKey) {
        return new EntityInit(smType, instanceKey, null, null, 0, List.of());
    }
}
