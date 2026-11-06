package io.concert.traceui;

import io.concert.common.WorkflowIds;
import io.concert.common.api.KeyLockWorkflow;
import io.concert.common.api.LockSnapshot;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.TaskQueueType;
import io.temporal.api.taskqueue.v1.TaskQueue;
import io.temporal.api.workflow.v1.PendingActivityInfo;
import io.temporal.api.workflow.v1.WorkflowExecutionInfo;
import io.temporal.api.workflowservice.v1.CountWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.DescribeTaskQueueRequest;
import io.temporal.api.workflowservice.v1.DescribeTaskQueueResponse;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.api.workflowservice.v1.ListWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc;
import io.temporal.client.WorkflowClient;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The coordinator page of a flow: what Temporal holds for it right now.
 *
 * <ul>
 *   <li><b>Serial key queues</b>: the live {@code KeyLockWorkflow} of each lock key the flow's events waited on in
 *       the window (holder, holder state, waiting events), busiest first;
 *   <li><b>Workflows</b>: the flow's entity (or event-processor) workflows by execution status, and the newest
 *       running ones;
 *   <li><b>Task queues</b>: pollers and backlog of {@code orchestration} and the flow's queue;
 *   <li><b>Ingest</b>: the ingest supervisor of the stream and its shard consumers (pending activities).
 * </ul>
 */
final class FlowTemporal {
    static final List<String> STATUSES = List.of("Running", "Completed", "ContinuedAsNew", "Failed", "Terminated", "TimedOut", "Canceled");

    private final WorkflowClient client;
    private final String temporalUi;
    private final String stream;

    FlowTemporal(WorkflowClient client, String temporalUi, String stream) {
        this.client = client;
        this.temporalUi = temporalUi;
        this.stream = stream;
    }

    private WorkflowServiceGrpc.WorkflowServiceBlockingStub stub() {
        return client.getWorkflowServiceStubs().blockingStub().withDeadlineAfter(3, TimeUnit.SECONDS);
    }

    private String ns() {
        return client.getOptions().getNamespace();
    }

    private final Map<String, Object[]> cache = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The view, cached for 4 s per flow: Temporal rate-limits visibility calls per namespace (10/s by default), and
     * several open pages refreshing would otherwise trip it.
     */
    Map<String, Object> view(FlowMetrics.Flow flow, Collection<String> lockKeys, String coordinatorQueue) {
        Object[] hit = cache.get(flow.id());
        if (hit != null && System.currentTimeMillis() - (long) hit[0] < 4000) {
            @SuppressWarnings("unchecked")
            Map<String, Object> v = (Map<String, Object>) hit[1];
            return v;
        }
        Map<String, Object> v = load(flow, lockKeys, coordinatorQueue);
        cache.put(flow.id(), new Object[] {System.currentTimeMillis(), v});
        return v;
    }

    private Map<String, Object> load(FlowMetrics.Flow flow, Collection<String> lockKeys, String coordinatorQueue) {
        Map<String, Object> out = new LinkedHashMap<>();
        boolean event = "event".equals(flow.style());
        String smType = event ? flow.id().substring(3) : flow.id();
        String base = "WorkflowType='" + (event ? "EventProcessorWorkflow" : "EntityWorkflow") + "' AND SmType='" + smType + "'";
        out.put("query", base);
        out.put("temporalUi", temporalUi + "/namespaces/" + ns() + "/workflows?query=" + URLEncoder.encode(base, StandardCharsets.UTF_8));
        out.put("locks", locks(lockKeys));
        out.put("openLocks", count("WorkflowType='KeyLockWorkflow' AND ExecutionStatus='Running'"));
        out.put("workflows", byStatus(base));
        out.put("running", running(base + " AND ExecutionStatus='Running'", 25));
        out.put("failed", running(base + " AND ExecutionStatus IN ('Failed','Terminated','TimedOut')", 10));
        List<Map<String, Object>> queues = new ArrayList<>();
        queues.add(queue(coordinatorQueue));
        queues.add(queue(flow.taskQueue()));
        out.put("taskQueues", queues);
        out.put("ingest", ingest());
        return out;
    }

    /** Live state of each lock key (in parallel, 3 s budget); keys with nothing held or waiting are counted as free. */
    List<Map<String, Object>> locks(Collection<String> keys) {
        List<Map<String, Object>> out = new ArrayList<>();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Map<String, Future<LockSnapshot>> futures = new LinkedHashMap<>();
            keys.stream().limit(60).forEach(k -> futures.put(k, pool.submit(
                    () -> client.newWorkflowStub(KeyLockWorkflow.class, WorkflowIds.lock(k)).snapshot())));
            long deadline = System.currentTimeMillis() + 3000;
            for (var e : futures.entrySet()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("lockKey", e.getKey());
                try {
                    LockSnapshot s = e.getValue().get(Math.max(1, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
                    row.put("holder", s.holder());
                    row.put("holderState", s.holderState());
                    row.put("waiting", s.waiting());
                    row.put("queued", s.waiting().size() + (s.holder() == null ? 0 : 1));
                } catch (ExecutionException ex) {
                    row.put("holder", null);
                    row.put("waiting", List.of());
                    row.put("queued", 0); // no open lock workflow: the key is free
                } catch (TimeoutException ex) {
                    e.getValue().cancel(true);
                    row.put("error", "timed out");
                    row.put("queued", 0);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
                row.put("temporalUi", temporalUi + "/namespaces/" + ns() + "/workflows/" + WorkflowIds.lock(e.getKey()));
                out.add(row);
            }
        }
        out.sort(Comparator.comparingInt((Map<String, Object> r) -> ((Number) r.get("queued")).intValue()).reversed());
        return out;
    }

    /** Workflow counts by execution status in one visibility call ({@code GROUP BY ExecutionStatus}). */
    private Map<String, Long> byStatus(String query) {
        Map<String, Long> out = new LinkedHashMap<>();
        STATUSES.forEach(st -> out.put(st, 0L));
        try {
            var r = stub().countWorkflowExecutions(CountWorkflowExecutionsRequest.newBuilder().setNamespace(ns())
                    .setQuery(query + " GROUP BY ExecutionStatus").build());
            for (var g : r.getGroupsList()) {
                String status = g.getGroupValuesCount() == 0 ? "?" : g.getGroupValues(0).getData().toStringUtf8().replace("\"", "");
                out.put(status, g.getCount());
            }
        } catch (RuntimeException e) {
            out.clear(); // visibility unavailable or rate-limited: the page says so
        }
        return out;
    }

    private Long count(String query) {
        try {
            return stub().countWorkflowExecutions(CountWorkflowExecutionsRequest.newBuilder().setNamespace(ns()).setQuery(query).build())
                    .getCount();
        } catch (RuntimeException e) {
            return null;
        }
    }

    private Object running(String query, int limit) {
        try {
            List<Map<String, Object>> out = new ArrayList<>();
            for (WorkflowExecutionInfo i : stub().listWorkflowExecutions(ListWorkflowExecutionsRequest.newBuilder()
                    .setNamespace(ns()).setQuery(query).setPageSize(limit).build()).getExecutionsList()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("workflowId", i.getExecution().getWorkflowId());
                m.put("status", i.getStatus().name().replace("WORKFLOW_EXECUTION_STATUS_", ""));
                m.put("startMillis", i.getStartTime().getSeconds() * 1000 + i.getStartTime().getNanos() / 1_000_000);
                m.put("historyLength", i.getHistoryLength());
                m.put("temporalUi", temporalUi + "/namespaces/" + ns() + "/workflows/" + i.getExecution().getWorkflowId()
                        + "/" + i.getExecution().getRunId());
                out.add(m);
            }
            return out;
        } catch (RuntimeException e) {
            return Map.of("error", String.valueOf(e.getMessage()));
        }
    }

    Map<String, Object> queue(String name) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name", name);
        for (TaskQueueType t : List.of(TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW, TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY)) {
            String k = t == TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW ? "workflow" : "activity";
            try {
                DescribeTaskQueueResponse r = stub().describeTaskQueue(DescribeTaskQueueRequest.newBuilder().setNamespace(ns())
                        .setTaskQueue(TaskQueue.newBuilder().setName(name).build()).setTaskQueueType(t).setReportStats(true).build());
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("pollers", r.getPollersCount());
                if (r.hasStats()) {
                    m.put("backlog", r.getStats().getApproximateBacklogCount());
                    m.put("addRate", Math.round(r.getStats().getTasksAddRate() * 10) / 10.0);
                    m.put("dispatchRate", Math.round(r.getStats().getTasksDispatchRate() * 10) / 10.0);
                }
                out.put(k, m);
            } catch (RuntimeException e) {
                out.put(k, Map.of("error", String.valueOf(e.getMessage())));
            }
        }
        return out;
    }

    /** The ingest supervisor of the stream and its shard consumers. */
    Map<String, Object> ingest() {
        Map<String, Object> out = new LinkedHashMap<>();
        String id = WorkflowIds.ingest(stream);
        out.put("workflowId", id);
        out.put("temporalUi", temporalUi + "/namespaces/" + ns() + "/workflows/" + id);
        try {
            DescribeWorkflowExecutionResponse r = stub().describeWorkflowExecution(DescribeWorkflowExecutionRequest.newBuilder()
                    .setNamespace(ns()).setExecution(WorkflowExecution.newBuilder().setWorkflowId(id).build()).build());
            out.put("status", r.getWorkflowExecutionInfo().getStatus().name().replace("WORKFLOW_EXECUTION_STATUS_", ""));
            out.put("historyLength", r.getWorkflowExecutionInfo().getHistoryLength());
            List<Map<String, Object>> acts = new ArrayList<>();
            for (PendingActivityInfo a : r.getPendingActivitiesList()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("activityId", a.getActivityId());
                m.put("type", a.getActivityType().getName());
                m.put("state", a.getState().name().replace("PENDING_ACTIVITY_STATE_", ""));
                m.put("attempt", a.getAttempt());
                m.put("lastHeartbeatMillis", a.hasLastHeartbeatTime()
                        ? a.getLastHeartbeatTime().getSeconds() * 1000 + a.getLastHeartbeatTime().getNanos() / 1_000_000 : null);
                m.put("lastFailure", a.hasLastFailure() ? a.getLastFailure().getMessage() : null);
                acts.add(m);
            }
            out.put("consumers", acts);
        } catch (RuntimeException e) {
            out.put("error", String.valueOf(e.getMessage()));
        }
        return out;
    }
}
