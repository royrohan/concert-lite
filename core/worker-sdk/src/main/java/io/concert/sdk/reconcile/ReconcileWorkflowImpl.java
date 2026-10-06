package io.concert.sdk.reconcile;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/** One heartbeating activity does the scan; retried a few times (it is idempotent). */
public final class ReconcileWorkflowImpl implements ReconcileWorkflow {

    private final ReconcileActivities activities = Workflow.newActivityStub(ReconcileActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofHours(1))
                    .setHeartbeatTimeout(Duration.ofMinutes(2))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(5))
                            .setMaximumAttempts(3)
                            .build())
                    .build());

    @Override
    public ReconcileReport run(ReconcileRequest request) {
        return activities.reconcile(request);
    }
}
