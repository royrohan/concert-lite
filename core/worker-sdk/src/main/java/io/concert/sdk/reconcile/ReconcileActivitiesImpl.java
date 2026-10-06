package io.concert.sdk.reconcile;

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;

/**
 * The activity behind {@link ReconcileWorkflow}, registered by {@code WorkerBootstrap} on the smType's
 * task queue for its own type only. Heartbeats the counts after every chunk.
 */
public final class ReconcileActivitiesImpl implements ReconcileActivities {

    private final String smType;
    private final Reconciler reconciler;
    /** Why this worker cannot reconcile (no Kafka configured), or {@code null}. */
    private final String unavailable;

    public ReconcileActivitiesImpl(String smType, Reconciler reconciler, String unavailable) {
        this.smType = smType;
        this.reconciler = reconciler;
        this.unavailable = unavailable;
    }

    @Override
    public ReconcileReport reconcile(ReconcileRequest request) {
        if (!smType.equals(request.smType())) {
            throw new IllegalArgumentException("this worker reconciles " + smType + ", not " + request.smType());
        }
        if (unavailable != null) {
            return ReconcileReport.skipped(smType, request.dryRun(), unavailable);
        }
        ActivityExecutionContext ctx = Activity.getExecutionContext();
        return reconciler.run(request, ctx::heartbeat);
    }
}
