package io.concert.common.api;

import java.util.List;

public record EntitySnapshot(
        String workflowId,
        String smType,
        String instanceKey,
        String state,
        String data,
        long version,
        long eventsHandledThisRun,
        List<TransitionRecord> recent) {}
