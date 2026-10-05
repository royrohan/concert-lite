package io.concert.common.api;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** One per stream (workflow id {@code ingest:<stream>}); keeps a consumer activity per open shard. */
@WorkflowInterface
public interface IngestSupervisorWorkflow {

    @WorkflowMethod
    void run(IngestConfig config);

    @QueryMethod
    IngestStatus status();
}
