package io.concert.traceui;

import io.concert.common.TaskQueues;
import io.concert.sdk.MachineCatalog;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.reconcile.ReconcileReport;
import io.concert.sdk.reconcile.ReconcileRequest;
import io.concert.sdk.reconcile.ReconcileWorkflow;
import io.temporal.api.enums.v1.TaskQueueType;
import io.temporal.api.taskqueue.v1.TaskQueue;
import io.temporal.api.workflowservice.v1.DescribeTaskQueueRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * On-demand reconciliation (Analytics tab button, {@code scripts/reconcile.sh}): starts one
 * {@link ReconcileWorkflow} per typed smType on that type's task queue, i.e. in the worker process that
 * owns the type, waits for all of them and returns their reports with totals. Types without a running
 * worker (no pollers on {@code sm-<type>}) are listed as skipped instead of waiting forever.
 */
final class ReconcileRunner {

    private final WorkflowClient client;
    private final Map<String, MachineCatalog.Machine> machines;
    private volatile Map<String, Object> last;

    ReconcileRunner(WorkflowClient client, Map<String, MachineCatalog.Machine> machines) {
        this.client = client;
        this.machines = machines;
    }

    /** The last on-demand result, or a hint. */
    Map<String, Object> last() {
        Map<String, Object> l = last;
        return l != null ? l : Map.of("reports", List.of(), "hint", "no reconciliation run from the trace UI yet");
    }

    /** @param smType one type, or {@code null} / blank for every typed machine with a running worker */
    Map<String, Object> run(String smType, boolean dryRun) {
        List<String> types = new ArrayList<>();
        List<Map<String, String>> skipped = new ArrayList<>();
        if (smType != null && !smType.isBlank()) {
            types.add(smType.trim());
        } else {
            machines.forEach((type, m) -> {
                if (ModelStateMachine.class.isAssignableFrom(m.impl())) {
                    types.add(type);
                }
            });
        }
        Map<String, CompletableFuture<ReconcileReport>> running = new LinkedHashMap<>();
        long started = System.currentTimeMillis();
        for (String type : types) {
            if (!hasWorker(type)) {
                skipped.add(Map.of("smType", type, "reason", "no worker polls " + TaskQueues.stateMachine(type)));
                continue;
            }
            ReconcileWorkflow wf = client.newWorkflowStub(ReconcileWorkflow.class, WorkflowOptions.newBuilder()
                    .setWorkflowId("reconcile-" + type + "-manual-" + started)
                    .setTaskQueue(TaskQueues.stateMachine(type))
                    .setWorkflowExecutionTimeout(Duration.ofMinutes(30))
                    .build());
            running.put(type, WorkflowClient.execute(wf::run, new ReconcileRequest(type, dryRun, ReconcileRequest.DEFAULT_CHUNK)));
        }
        List<Object> reports = new ArrayList<>();
        long[] totals = new long[6];
        for (var e : running.entrySet()) {
            try {
                ReconcileReport r = e.getValue().get(Math.max(1, 120_000 - (System.currentTimeMillis() - started)), TimeUnit.MILLISECONDS);
                reports.add(r);
                if (r.skipped() != null) {
                    skipped.add(Map.of("smType", r.smType(), "reason", r.skipped()));
                }
                totals[0] += r.scanned();
                totals[1] += r.terminal();
                totals[2] += r.missing();
                totals[3] += r.stale();
                totals[4] += r.republished();
                totals[5] += r.ahead();
            } catch (ExecutionException ex) {
                reports.add(Map.of("smType", e.getKey(), "error", String.valueOf(ex.getCause())));
            } catch (TimeoutException ex) {
                reports.add(Map.of("smType", e.getKey(), "error", "still running after 120 s; see the Temporal UI"));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("at", Instant.now().toString());
        out.put("dryRun", dryRun);
        out.put("totals", Map.of("scanned", totals[0], "terminal", totals[1], "missing", totals[2], "stale", totals[3],
                "republished", totals[4], "ahead", totals[5]));
        out.put("reports", reports);
        out.put("skipped", skipped);
        out.put("elapsedMs", System.currentTimeMillis() - started);
        last = out;
        return out;
    }

    private boolean hasWorker(String smType) {
        try {
            return client.getWorkflowServiceStubs().blockingStub().describeTaskQueue(DescribeTaskQueueRequest.newBuilder()
                    .setNamespace(client.getOptions().getNamespace())
                    .setTaskQueue(TaskQueue.newBuilder().setName(TaskQueues.stateMachine(smType)).build())
                    .setTaskQueueType(TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY)
                    .build()).getPollersCount() > 0;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
