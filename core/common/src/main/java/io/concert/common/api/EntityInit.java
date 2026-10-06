package io.concert.common.api;

import java.util.List;

/**
 * Start (or continue-as-new) input of an entity workflow. {@code state == null} means fresh.
 *
 * @param createdAtMillis workflow time of the entity's first accepted transition, carried across
 *     continue-as-new for the completed-entity snapshot; 0 if none yet (or unknown: inputs written
 *     before this field existed deserialize to 0)
 */
public record EntityInit(
        String smType, String instanceKey, String state, String data, long version, List<TransitionRecord> recent,
        long createdAtMillis) {

    public static EntityInit fresh(String smType, String instanceKey) {
        return new EntityInit(smType, instanceKey, null, null, 0, List.of(), 0);
    }
}
