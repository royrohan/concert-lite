package io.concert.common.api;

/** Projection of an entity's state into DSQL {@code sm_state}. */
public record SmStateRow(
        String workflowId, String smType, String state, String data, long version, String lastEventId, long updatedAtMillis) {}
