package io.concert.dashboard;

import io.concert.common.Env;
import io.concert.orchestration.KinesisStreams;
import io.concert.sdk.reconcile.ReconcileSchedules;
import io.concert.sink.SnapshotTopics;
import io.concert.store.StateStore;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionMetadata;
import io.temporal.client.schedules.ScheduleClient;
import io.temporal.client.schedules.ScheduleClientOptions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

/**
 * Start / Stop / Delete of one flow (ecosystem) and the shared-services switch, run as background jobs whose log the
 * pages poll.
 *
 * <ul>
 *   <li><b>Start</b>: brings up Kinesis (LocalStack) and Kafka if they are down, creates the flow's stream
 *       {@code concert-<flow>} and topic {@code snapshots.<flow>}, then starts its coordinator and workers (building
 *       images that don't exist yet). If Deephaven runs and the topic is new, restarts it so it reads the topic.
 *   <li><b>Stop</b>: stops its data generators and its containers; the stream, topic, workflows and rows stay.
 *   <li><b>Delete</b>: Stop, removes the containers, terminates the flow's running workflows (its worker task queues
 *       and orchestration queue) and reconcile schedules, deletes its stream and topic and purges its state, trace and
 *       checkpoint rows. Analytics tables are kept. The flow stays registered, so Start recreates it empty.
 * </ul>
 *
 * The shared services are those every flow uses: Kinesis (LocalStack), Kafka, the DuckDB sink, ClickHouse and
 * legend-lite (with Legend Studio).
 */
final class FlowOps {
    private static final Logger log = LoggerFactory.getLogger(FlowOps.class);

    /** The shared services the main page switches, as {label, service}; one-shot provisioners follow their store. */
    static final List<String[]> SHARED = List.of(
            new String[] {"Kinesis", "localstack"},
            // CloudWatch metrics + Logs run in the same LocalStack; its pill reads LocalStack's health (see shared())
            new String[] {"CloudWatch", "localstack"},
            new String[] {"Kafka", "kafka"},
            new String[] {"DuckDB", "sink-duckdb"},
            new String[] {"ClickHouse", "clickhouse"},
            new String[] {"legend-lite", "legend-lite"},
            new String[] {"Legend Studio", "legend-studio"});
    private static final List<String> SHARED_ONE_SHOT = List.of("kafka-init", "sink-clickhouse");

    /** A background operation and its log. */
    static final class Job {
        final String action;
        final long startedAt = System.currentTimeMillis();
        final List<String> lines = new ArrayList<>();
        volatile String state = "RUNNING";
        volatile Long endedAt;

        Job(String action) {
            this.action = action;
        }

        synchronized void log(String line) {
            lines.add(Instant.now().toString().substring(11, 19) + "  " + line);
            if (lines.size() > 400) {
                lines.remove(0);
            }
        }

        synchronized Map<String, Object> view() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("action", action);
            m.put("state", state);
            m.put("startedAt", startedAt);
            m.put("endedAt", endedAt);
            m.put("log", List.copyOf(lines));
            return m;
        }
    }

    private final FlowRegistry registry;
    private final DockerCompose compose;
    private final KinesisClient kinesis;
    private final WorkflowClient client;
    private final StateStore store;
    private final FlowGenerator generator;
    private final String kafka = Env.get("KAFKA_BOOTSTRAP", "");
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private volatile Object[] containersCache;
    /** LocalStack's CloudWatch services: {@code {cloudwatch, logs}} states ({@link CloudWatchMetrics#status}). */
    private volatile java.util.function.Supplier<Map<String, String>> cloudWatchStatus = Map::of;

    FlowOps(FlowRegistry registry, DockerCompose compose, KinesisClient kinesis, WorkflowClient client, StateStore store,
            FlowGenerator generator) {
        this.registry = registry;
        this.compose = compose;
        this.kinesis = kinesis;
        this.client = client;
        this.store = store;
        this.generator = generator;
    }

    DockerCompose compose() {
        return compose;
    }

    FlowRegistry registry() {
        return registry;
    }

    // ---------------------------------------------------------------- status

    /** Every flow with its state ({@link #flow}), for the landing page. */
    Map<String, Object> flows() {
        Map<String, Map<String, String>> containers = containersOrEmpty();
        List<Map<String, Object>> out = new ArrayList<>();
        for (FlowRegistry.Flow f : registry.all()) {
            out.add(flow(f, containers, false));
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("flows", out);
        m.put("docker", compose.unavailable());
        return m;
    }

    Map<String, Object> flow(String id) {
        return flow(registry.get(id), containersOrEmpty(), true);
    }

    private Map<String, Object> flow(FlowRegistry.Flow f, Map<String, Map<String, String>> containers, boolean detail) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", f.id());
        m.put("label", f.label());
        m.put("coordinatorImpl", f.coordinatorImpl());
        m.put("machines", f.machines());
        m.put("machineFlows", f.machineFlows());
        m.put("eventCatalogs", f.eventCatalogs());
        List<Map<String, Object>> services = new ArrayList<>();
        int running = 0;
        int present = 0;
        for (String s : f.services()) {
            Map<String, String> c = containers.get(s);
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("name", s);
            x.put("state", c == null ? "absent" : c.get("state"));
            x.put("status", c == null ? "" : c.get("status"));
            services.add(x);
            present += c == null ? 0 : 1;
            running += c != null && "running".equals(c.get("state")) ? 1 : 0;
        }
        m.put("services", services);
        m.put("state", running == f.services().size() ? "running" : running > 0 ? "partial" : present > 0 ? "stopped" : "absent");
        m.put("stream", f.stream());
        m.put("topic", f.topic());
        m.put("orchestrationQueue", f.orchestrationQueue());
        m.put("taskQueues", f.taskQueues());
        if (detail) {
            m.put("streamStatus", cachedStreamStatus(f.stream()));
        }
        Job j = jobs.get(f.id());
        m.put("job", j == null ? null : j.view());
        return m;
    }

    void cloudWatchStatus(java.util.function.Supplier<Map<String, String>> status) {
        this.cloudWatchStatus = status;
    }

    /** The shared services with their containers' states; {@code state} up / partial / down. */
    Map<String, Object> shared() {
        Map<String, Map<String, String>> containers = containersOrEmpty();
        List<Map<String, Object>> services = new ArrayList<>();
        int up = 0;
        for (String[] s : SHARED) {
            Map<String, String> c = containers.get(s[1]);
            boolean running = c != null && "running".equals(c.get("state"));
            String health = c == null ? "" : c.get("health");
            String note = null;
            if (running && s[0].equals("CloudWatch")) {
                Map<String, String> cw = cloudWatchStatus.get();
                String metrics = cw.getOrDefault("cloudwatch", ""), logs = cw.getOrDefault("logs", "");
                if (metrics.equals("disabled") || logs.equals("disabled")) {
                    health = "unhealthy";
                    note = "LocalStack runs without CloudWatch (an older container): recreate it from the repo with "
                            + "`docker compose up -d localstack` (Kinesis streams are recreated empty by the coordinators)";
                } else if (metrics.equals("unreachable")) {
                    health = "unhealthy";
                    note = "LocalStack's health endpoint does not answer";
                }
            }
            up += running ? 1 : 0;
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("label", s[0]);
            x.put("service", s[1]);
            x.put("state", c == null ? "absent" : c.get("state"));
            x.put("health", health);
            if (note != null) {
                x.put("note", note);
            }
            services.add(x);
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("services", services);
        m.put("state", up == SHARED.size() ? "up" : up == 0 ? "down" : "partial");
        m.put("docker", compose.unavailable());
        Job j = jobs.get("shared");
        m.put("job", j == null ? null : j.view());
        return m;
    }

    private Map<String, Map<String, String>> containersOrEmpty() {
        Object[] c = containersCache;
        if (c != null && System.currentTimeMillis() - (long) c[0] < 2000) {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, String>> m = (Map<String, Map<String, String>>) c[1];
            return m;
        }
        Map<String, Map<String, String>> m;
        try {
            m = compose.unavailable() == null ? compose.containers() : Map.of();
        } catch (RuntimeException e) {
            log.warn("container status: {}", e.getMessage());
            m = Map.of();
        }
        containersCache = new Object[] {System.currentTimeMillis(), m};
        return m;
    }

    private final Map<String, Object[]> streamCache = new ConcurrentHashMap<>();

    private String cachedStreamStatus(String stream) {
        Object[] c = streamCache.get(stream);
        if (c != null && System.currentTimeMillis() - (long) c[0] < 5000) {
            return (String) c[1];
        }
        String s;
        try {
            StreamStatus st = KinesisStreams.status(kinesis, stream);
            s = st == null ? "absent" : st.toString();
        } catch (RuntimeException e) {
            s = "unknown (" + e.getMessage() + ")";
        }
        streamCache.put(stream, new Object[] {System.currentTimeMillis(), s});
        return s;
    }

    // ---------------------------------------------------------------- jobs

    Map<String, Object> start(String id) {
        FlowRegistry.Flow f = registry.get(id);
        return submit(id, "start", job -> bringUp(f, job, 0, 0, Map.of()));
    }

    /**
     * Start's work: Kinesis and Kafka, the flow's stream and topic, then its coordinator and workers. With
     * {@code coordinators} / {@code workers} above 0 each runs that many replicas ({@code --scale}); {@code tuning} sets
     * {@link DockerCompose#TUNING_ENV} variables for the started containers.
     */
    void bringUp(FlowRegistry.Flow f, Job job, int coordinators, int workers, Map<String, String> tuning) {
        ensureKinesisAndKafka(job);
        job.log("stream " + f.stream() + ": " + (KinesisStreams.ensure(kinesis, f.stream(), Env.getInt("KINESIS_SHARDS", 2), 120_000)
                ? "created" : "exists"));
        boolean newTopic = false;
        if (kafka.isBlank()) {
            job.log("KAFKA_BOOTSTRAP not set: no snapshot topic (workers log snapshots instead)");
        } else {
            newTopic = SnapshotTopics.ensure(kafka, f.topic());
            job.log("topic " + f.topic() + ": " + (newTopic ? "created" : "exists"));
        }
        job.log("starting " + String.join(", ", f.services()) + " (images are built if missing: the first start takes a few minutes)");
        // --no-deps: Kinesis and Kafka are ensured above; never recreate a shared service from here
        List<String> args = new ArrayList<>(List.of("up", "-d", "--no-deps"));
        if (coordinators > 0) {
            args.addAll(List.of("--scale", f.coordinator() + "=" + coordinators));
        }
        if (workers > 0) {
            f.workers().forEach(w -> args.addAll(List.of("--scale", w + "=" + workers)));
        }
        if (!tuning.isEmpty()) {
            job.log("tuning " + tuning);
        }
        args.addAll(f.services());
        check(job, compose.compose(profiles(f), args, job::log, 1800, tuning), "docker compose up");
        if (newTopic && "running".equals(containersOrEmptyFresh().getOrDefault("deephaven", Map.of()).get("state"))) {
            job.log("restarting Deephaven so it reads " + f.topic());
            compose.compose(List.of("analytics"), List.of("restart", "deephaven"), job::log, 300);
        }
    }

    /** Stops the data generators and containers of these flows in one compose call (Stop, for several flows). */
    void stopFlows(List<FlowRegistry.Flow> flows, Job job) {
        if (flows.isEmpty()) {
            return;
        }
        List<String> services = new ArrayList<>();
        java.util.Set<String> profiles = new java.util.LinkedHashSet<>(List.of("analytics"));
        for (FlowRegistry.Flow f : flows) {
            stopGenerators(f, job);
            services.addAll(f.services());
            profiles.add(f.profile());
        }
        job.log("stopping " + String.join(", ", services));
        List<String> args = new ArrayList<>(List.of("stop"));
        args.addAll(services);
        check(job, compose.compose(List.copyOf(profiles), args, job::log, 300), "docker compose stop");
    }

    Map<String, Object> stop(String id) {
        FlowRegistry.Flow f = registry.get(id);
        return submit(id, "stop", job -> {
            stopGenerators(f, job);
            List<String> args = new ArrayList<>(List.of("stop"));
            args.addAll(f.services());
            check(job, compose.compose(profiles(f), args, job::log, 300), "docker compose stop");
        });
    }

    Map<String, Object> delete(String id) {
        FlowRegistry.Flow f = registry.get(id);
        return submit(id, "delete", job -> {
            stopGenerators(f, job);
            List<String> args = new ArrayList<>(List.of("rm", "-s", "-f"));
            args.addAll(f.services());
            check(job, compose.compose(profiles(f), args, job::log, 300), "docker compose rm");
            terminateWorkflows(f, job);
            deleteSchedules(f, job);
            job.log("stream " + f.stream() + ": " + (safe(() -> KinesisStreams.delete(kinesis, f.stream()), job) == Boolean.TRUE
                    ? "deleted" : "not found"));
            if (!kafka.isBlank()) {
                job.log("topic " + f.topic() + ": " + (safe(() -> SnapshotTopics.delete(kafka, f.topic()), job) == Boolean.TRUE
                        ? "deleted" : "not found"));
            }
            try {
                long n = store.purgeFlow(f.smTypes(), f.workflowPrefixes(), f.stream());
                job.log("purged " + n + " state / trace / checkpoint rows (" + store.kind() + "); analytics tables kept");
            } catch (UnsupportedOperationException e) {
                job.log(e.getMessage() + ": state rows kept");
            }
        });
    }

    /** Starts (or stops) every shared service. */
    Map<String, Object> shared(boolean up) {
        return submit("shared", up ? "start" : "stop", job -> {
            List<String> services = new ArrayList<>(SHARED.stream().map(s -> s[1]).distinct().toList());
            if (up) {
                services.addAll(SHARED_ONE_SHOT);
                List<String> args = new ArrayList<>(List.of("up", "-d", "--no-recreate"));
                args.addAll(services);
                check(job, compose.compose(List.of("analytics"), args, job::log, 1800), "docker compose up");
            } else {
                List<String> args = new ArrayList<>(List.of("stop"));
                args.addAll(services);
                check(job, compose.compose(List.of("analytics"), args, job::log, 300), "docker compose stop");
            }
        });
    }

    Map<String, Object> job(String key) {
        Job j = jobs.get(key);
        return j == null ? Map.of() : j.view();
    }

    interface Body {
        void run(Job job) throws Exception;
    }

    synchronized Map<String, Object> submit(String key, String action, Body body) {
        String why = compose.unavailable();
        if (why != null) {
            throw new IllegalStateException("cannot " + action + ": " + why);
        }
        Job running = jobs.get(key);
        if (running != null && "RUNNING".equals(running.state)) {
            throw new IllegalStateException(running.action + " of " + key + " is still running");
        }
        Job job = new Job(action);
        jobs.put(key, job);
        job.log(action + " " + key);
        Thread.ofVirtual().name("flow-" + action + "-" + key).start(() -> {
            try {
                body.run(job);
                job.state = "DONE";
                job.log("done");
            } catch (Exception e) {
                job.state = "FAILED";
                job.log("failed: " + e.getMessage());
                log.warn("{} {} failed", action, key, e);
            } finally {
                job.endedAt = System.currentTimeMillis();
                containersCache = null;
            }
        });
        return job.view();
    }

    void ensureKinesisAndKafka(Job job) {
        Map<String, Map<String, String>> c = containersOrEmptyFresh();
        List<String> down = new ArrayList<>();
        for (String s : kafka.isBlank() ? List.of("localstack") : List.of("localstack", "kafka")) {
            if (!"running".equals(c.getOrDefault(s, Map.of()).get("state"))) {
                down.add(s);
            }
        }
        if (down.isEmpty()) {
            job.log("Kinesis and Kafka are up");
        } else {
            job.log("bringing up " + String.join(", ", down));
        }
        // --no-recreate: a compose version other than the host's hashes the configuration differently and would
        // recreate them (LocalStack keeps streams in memory: every flow's stream would be lost)
        List<String> args = new ArrayList<>(List.of("up", "-d", "--no-recreate", "--wait", "--wait-timeout", "240", "localstack"));
        if (!kafka.isBlank()) {
            args.add("kafka");
        }
        check(job, compose.compose(List.of("analytics"), args, down.isEmpty() ? l -> { } : job::log, 300), "starting Kinesis / Kafka");
        if (down.contains("kafka")) {
            compose.compose(List.of("analytics"), List.of("up", "-d", "--no-deps", "kafka-init"), job::log, 300); // shared topics
        }
    }

    Map<String, Map<String, String>> containersOrEmptyFresh() {
        containersCache = null;
        return containersOrEmpty();
    }

    void stopGenerators(FlowRegistry.Flow f, Job job) {
        for (String mf : f.machineFlows()) {
            Object run = generator.status(mf).get("run");
            if (run instanceof Map<?, ?> r && Boolean.TRUE.equals(r.get("running"))) {
                generator.stop(mf);
                job.log("stopped the data generator of " + mf);
            }
        }
    }

    private void terminateWorkflows(FlowRegistry.Flow f, Job job) {
        int n = 0;
        for (String q : f.taskQueues()) {
            String query = "ExecutionStatus = 'Running' AND TaskQueue = '" + q + "'";
            try {
                List<WorkflowExecutionMetadata> running = client.listExecutions(query).toList();
                for (WorkflowExecutionMetadata w : running) {
                    try {
                        client.newUntypedWorkflowStub(w.getExecution().getWorkflowId()).terminate("Delete flow " + f.id());
                        n++;
                    } catch (RuntimeException e) {
                        job.log("terminate " + w.getExecution().getWorkflowId() + ": " + e.getMessage());
                    }
                }
            } catch (RuntimeException e) {
                job.log("listing workflows of " + q + ": " + e.getMessage());
            }
        }
        job.log("terminated " + n + " running workflows");
    }

    private void deleteSchedules(FlowRegistry.Flow f, Job job) {
        try {
            ScheduleClient schedules = ScheduleClient.newInstance(client.getWorkflowServiceStubs(),
                    ScheduleClientOptions.newBuilder().setNamespace(client.getOptions().getNamespace()).build());
            for (String smType : f.machines()) {
                try {
                    schedules.getHandle(ReconcileSchedules.scheduleId(smType)).delete();
                    job.log("deleted schedule " + ReconcileSchedules.scheduleId(smType));
                } catch (RuntimeException e) {
                    // none
                }
            }
        } catch (RuntimeException e) {
            job.log("schedules: " + e.getMessage());
        }
    }

    /** The flow's profile plus analytics (Kafka), so its services and their dependencies resolve. */
    static List<String> profiles(FlowRegistry.Flow f) {
        return List.of(f.profile(), "analytics");
    }

    static void check(Job job, int rc, String what) {
        if (rc != 0) {
            throw new IllegalStateException(what + " exited with " + rc);
        }
    }

    private static <T> T safe(Supplier<T> s, Job job) {
        try {
            return s.get();
        } catch (RuntimeException e) {
            job.log(e.getMessage());
            return null;
        }
    }
}
