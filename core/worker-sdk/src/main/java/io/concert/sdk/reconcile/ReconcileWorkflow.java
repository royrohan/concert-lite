package io.concert.sdk.reconcile;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Compares the terminal entities of one smType in the StateStore with the DuckDB sink and republishes
 * the current snapshot of missing or stale ones (see {@link Reconciler}). Runs on the smType's own task
 * queue ({@code sm-<type>}), in the worker process that owns the type: that process has the spec (terminal
 * states), the store and the snapshot publisher. Started hourly by the schedule {@code reconcile-<type>}
 * ({@link ReconcileSchedules}) and on demand by the trace UI ({@code scripts/reconcile.sh}).
 */
@WorkflowInterface
public interface ReconcileWorkflow {

    @WorkflowMethod(name = "ReconcileWorkflow")
    ReconcileReport run(ReconcileRequest request);
}
