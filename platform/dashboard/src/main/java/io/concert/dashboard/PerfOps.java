package io.concert.dashboard;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Performance page: one flow at a time, measured on its own.
 *
 * <ul>
 *   <li><b>Start</b>: stops every other flow (their data generators and containers) and, if asked, the analytics
 *       services that only add load (DuckDB sink, ClickHouse, Deephaven, Legend, market data); then starts the chosen
 *       flow with a {@link Preset}: that many coordinator and worker replicas ({@code docker compose --scale}) and the
 *       preset's Temporal / worker tuning; then generates load on one of its machines and, if asked, starts chaos.
 *   <li><b>Instances</b>: kill (SIGKILL), stop (SIGTERM, graceful) or start any coordinator or worker replica, and stop
 *       or start Kafka, to see how the flow behaves; every action goes on the run's timeline.
 *   <li><b>Chaos</b>: every few seconds one random victim (a coordinator or worker replica, or Kafka if included) goes
 *       down for 3 to 12 seconds, killed or stopped gracefully, then comes back.
 *   <li><b>Stop</b>: stops the load and chaos and brings back whatever is down; the flow keeps running.
 * </ul>
 *
 * The page shows the run's machine the way a state machine flow page does (completions, latency, pipeline), so a run
 * reads like any flow.
 */
final class PerfOps {
    private static final Logger log = LoggerFactory.getLogger(PerfOps.class);
    static final String JOB = "perf";

    /** Services that only consume snapshots or serve queries: stopped on request so they don't compete for CPU. */
    static final List<String> ANALYTICS = List.of("sink-duckdb", "clickhouse", "deephaven", "legend-lite", "legend-studio",
            "marketdata-sim", "redpanda-console");
    private static final List<String> ANALYTICS_PROFILES = List.of("analytics", "kafka-ui");

    /**
     * A sample Temporal / coordinator / worker configuration.
     *
     * @param tuning {@link DockerCompose#TUNING_ENV} values for the flow's containers (absent = the code's defaults)
     */
    record Preset(String id, String label, int coordinators, int workers, double rate, Map<String, String> tuning, String note) {}

    static final List<Preset> PRESETS = List.of(
            new Preset("smoke", "Smoke: 1 coordinator, 1 worker", 1, 1, 5, Map.of(),
                    "Defaults everywhere, light load: checks the flow end to end."),
            new Preset("balanced", "Balanced: 2 coordinators, 2 workers", 2, 2, 20, Map.of(),
                    "Two of each, so either can die without stopping the flow (the chaos baseline)."),
            new Preset("scale-out", "Scale-out: 3 coordinators, 4 workers", 3, 4, 50,
                    Map.of("WORKER_WF_SLOTS", "2000", "WORKER_LA_SLOTS", "4000", "ORCH_WF_POLLERS_MAX", "128",
                            "INGEST_PARALLEL_GROUPS", "512"),
                    "More replicas, slots and pollers: how far the flow scales on this machine."),
            new Preset("fast-failover", "Fast failover: 2 coordinators, 2 workers, 1 s task timeout", 2, 2, 20,
                    Map.of("WORKFLOW_TASK_TIMEOUT_MS", "1000"),
                    "Workflow tasks of a dead process move after 1 s instead of 2 s; more redone work when a process only pauses."),
            new Preset("constrained", "Constrained: 1 coordinator, 2 workers, few slots", 1, 2, 20,
                    Map.of("WORKER_WF_SLOTS", "50", "WORKER_LA_SLOTS", "100", "ORCH_WF_POLLERS_MAX", "4",
                            "INGEST_PARALLEL_GROUPS", "16"),
                    "Small worker slots and coordinator parallelism: shows backpressure and queueing."));

    /**
     * How events are serialized: {@code serial} with the catalog's lock keys (e.g. an order's account) through the lock
     * workflows; {@code entity} with only their entity's key, still through the lock workflows; {@code direct} with only
     * their entity's key and no lock workflows at all (the coordinators' {@code DIRECT_SM_TYPES}: dispatched straight to
     * the entity, ordered by the shard). Events that workers emit (chains) always take the lock workflows.
     */
    static final List<String> LOCK_MODES = List.of("serial", "entity", "direct");

    /** Metrics sample period: consecutive windows of this length, so counts add up over a run. */
    static final int SAMPLE_SEC = 10;
    /** After the load ends, samples continue this long (the backlog drains), then the run completes. */
    static final int DRAIN_SEC = 30;

    /** One timeline entry. */
    record Mark(long ts, String action, String target, String detail) {}

    /** The current run's settings and timeline. */
    static final class Run {
        final String runId;
        final String coordinatorImpl;
        final String lockMode;
        final Map<String, String> tuning;
        final PerfStore store;
        volatile String status = "running";
        volatile long loadStartedAt;
        final String flow;
        final String machine;
        final Preset preset;
        final int coordinators;
        final int workers;
        final double rate;
        final int durationSec;
        final int chaosEverySec;
        final boolean chaosKafka;
        final long startedAt = System.currentTimeMillis();
        volatile boolean chaosOn;
        volatile boolean kafkaStoppedByUs;
        final List<Mark> timeline = new ArrayList<>();

        Run(String flow, String coordinatorImpl, String machine, Preset preset, int coordinators, int workers, double rate,
                int durationSec, boolean chaos, int chaosEverySec, boolean chaosKafka, String lockMode, Map<String, String> tuning,
                PerfStore store) {
            this.runId = "perf-" + flow + "-" + java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
            this.coordinatorImpl = coordinatorImpl;
            this.lockMode = lockMode;
            this.tuning = Map.copyOf(tuning);
            this.store = store;
            this.flow = flow;
            this.machine = machine;
            this.preset = preset;
            this.coordinators = coordinators;
            this.workers = workers;
            this.rate = rate;
            this.durationSec = durationSec;
            this.chaosOn = chaos;
            this.chaosEverySec = chaosEverySec;
            this.chaosKafka = chaosKafka;
        }

        synchronized void mark(String action, String target, String detail) {
            Mark m = new Mark(System.currentTimeMillis(), action, target, detail);
            timeline.add(m);
            if (timeline.size() > 200) {
                timeline.remove(0);
            }
            if (store != null) {
                try {
                    store.addMark(runId, m.ts(), action, target, detail);
                } catch (java.sql.SQLException | RuntimeException e) {
                    log.warn("perf mark of {} not stored: {}", runId, e.getMessage());
                }
            }
        }

        synchronized List<Mark> marks() {
            return List.copyOf(timeline);
        }
    }

    private final FlowOps ops;
    private final FlowGenerator generator;
    private final FlowMetrics metrics;
    /** Past runs (DuckDB); {@code null} when the file can't be opened: runs still work, nothing is kept. */
    private final PerfStore store;
    private volatile Run run;
    private volatile Thread chaosThread;

    PerfOps(FlowOps ops, FlowGenerator generator, FlowMetrics metrics, PerfStore store) {
        this.ops = ops;
        this.generator = generator;
        this.metrics = metrics;
        this.store = store;
    }

    // ---------------------------------------------------------------- status

    Map<String, Object> view() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("presets", PRESETS);
        m.put("lockModes", LOCK_MODES);
        m.put("store", store == null ? null : store.file().toString());
        List<Map<String, Object>> flows = new ArrayList<>();
        for (FlowRegistry.Flow f : ops.registry().all()) {
            if (f.machines().isEmpty()) {
                continue; // the page measures a state machine flow
            }
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("id", f.id());
            x.put("label", f.label());
            x.put("coordinatorImpl", f.coordinatorImpl());
            x.put("machines", f.machines());
            x.put("defaultMachine", defaultMachine(f));
            flows.add(x);
        }
        m.put("flows", flows);
        m.put("docker", ops.compose().unavailable());
        m.put("job", ops.job(JOB));
        Run r = run;
        m.put("run", r == null ? null : runView(r));
        Map<String, Map<String, String>> containers = ops.containersOrEmptyFresh();
        m.put("kafka", containers.getOrDefault("kafka", Map.of("state", "absent")).getOrDefault("state", "absent"));
        List<Map<String, String>> analytics = new ArrayList<>();
        for (String s : ANALYTICS) {
            analytics.add(Map.of("service", s, "state", containers.getOrDefault(s, Map.of("state", "absent")).getOrDefault("state", "absent")));
        }
        m.put("analytics", analytics);
        List<Map<String, Object>> others = new ArrayList<>();
        for (FlowRegistry.Flow f : ops.registry().all()) {
            boolean up = f.services().stream().anyMatch(s -> "running".equals(containers.getOrDefault(s, Map.of()).get("state")));
            if (up && (r == null || !f.id().equals(r.flow))) {
                others.add(Map.of("id", f.id(), "label", f.label()));
            }
        }
        m.put("otherFlowsRunning", others);
        m.put("instances", r == null ? List.of() : instances(ops.registry().get(r.flow)));
        return m;
    }

    private Map<String, Object> runView(Run r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("runId", r.runId);
        m.put("status", r.status);
        m.put("lockMode", r.lockMode);
        m.put("tuning", r.tuning);
        m.put("flow", r.flow);
        m.put("machine", r.machine);
        m.put("preset", r.preset);
        m.put("coordinators", r.coordinators);
        m.put("workers", r.workers);
        m.put("rate", r.rate);
        m.put("durationSec", r.durationSec);
        m.put("startedAt", r.startedAt);
        Map<String, Object> chaos = new LinkedHashMap<>();
        chaos.put("on", r.chaosOn);
        chaos.put("everySec", r.chaosEverySec);
        chaos.put("kafka", r.chaosKafka);
        m.put("chaos", chaos);
        m.put("timeline", r.marks());
        m.put("generator", generator.status(r.machine).get("run"));
        return m;
    }

    /** The flow's coordinator and worker replicas with their role. */
    private List<Map<String, Object>> instances(FlowRegistry.Flow f) {
        List<Map<String, Object>> out = new ArrayList<>();
        try {
            for (Map<String, String> c : ops.compose().replicas(f.services())) {
                Map<String, Object> x = new LinkedHashMap<>(c);
                x.put("role", c.get("service").equals(f.coordinator()) ? "coordinator" : "worker");
                out.add(x);
            }
        } catch (RuntimeException e) {
            log.warn("replicas of {}: {}", f.id(), e.getMessage());
        }
        return out;
    }

    /**
     * The machine a run loads by default: the flow's first machine that is not a chain entry point ({@code *_system}). A
     * chain entry's emitted events all share the sample's account, so every instance would queue on one lock key; a
     * machine's own lock fields are made unique per instance by the generator.
     */
    static String defaultMachine(FlowRegistry.Flow f) {
        return f.machines().stream().filter(m -> !m.endsWith("_system")).findFirst().orElse(f.machines().getFirst());
    }

    // ---------------------------------------------------------------- run

    /**
     * Starts a run (stopping the previous one): every other flow stops, then the flow comes up with the preset and load
     * starts. {@code coordinators}, {@code workers} and {@code rate} override the preset's when above 0.
     */
    Map<String, Object> start(String flowId, String presetId, String machine, int coordinators, int workers, double rate,
            int durationSec, boolean chaos, int chaosEverySec, boolean chaosKafka, boolean stopAnalytics, String lockMode) {
        if (!LOCK_MODES.contains(lockMode)) {
            throw new IllegalArgumentException("lock mode must be one of " + LOCK_MODES + ": " + lockMode);
        }
        FlowRegistry.Flow f = ops.registry().get(flowId);
        if (f.machines().isEmpty()) {
            throw new IllegalArgumentException(flowId + " has no state machine to load");
        }
        Preset p = PRESETS.stream().filter(x -> x.id().equals(presetId)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown preset " + presetId));
        String m = machine == null || machine.isBlank() ? defaultMachine(f) : machine;
        if (!f.machines().contains(m)) {
            throw new IllegalArgumentException(m + " is not a machine of " + flowId + " " + f.machines());
        }
        if (generator.strategy(m) == null) {
            throw new IllegalArgumentException("no data generator for " + m);
        }
        int c = coordinators > 0 ? coordinators : p.coordinators();
        int w = workers > 0 ? workers : p.workers();
        double r = rate > 0 ? rate : p.rate();
        Map<String, String> tuning = new java.util.TreeMap<>(p.tuning());
        if (lockMode.equals("direct")) {
            tuning.put("DIRECT_SM_TYPES", String.join(",", f.machines()));
        }
        Run next = new Run(f.id(), f.coordinatorImpl(), m, p, Math.min(c, 8), Math.min(w, 8), Math.min(r, 200),
                Math.max(30, Math.min(durationSec, 3600)), chaos, Math.max(5, chaosEverySec), chaosKafka, lockMode, tuning, store);
        return ops.submit(JOB, "start", job -> {
            endRun(job, "stopped");
            run = next;
            if (store != null) {
                try {
                    store.startRun(next.runId, f.id(), f.coordinatorImpl(), m, p.id(), next.coordinators, next.workers, next.rate,
                            next.durationSec, lockMode, chaos, next.chaosEverySec, chaosKafka,
                            io.concert.common.Json.write(tuning), next.startedAt);
                    job.log("run " + next.runId + " is recorded in " + store.file());
                } catch (java.sql.SQLException | RuntimeException e) {
                    job.log("run not recorded: " + e.getMessage());
                }
            }
            next.mark("START", f.id(), p.label().split(":")[0] + " preset: " + next.coordinators + " coordinator(s), " + next.workers
                    + " worker(s), " + next.rate + " instances/s on " + m + ", lock mode " + lockMode);
            List<FlowRegistry.Flow> others = ops.registry().all().stream().filter(x -> !x.id().equals(f.id())).toList();
            job.log("one flow at a time: stopping every other flow");
            ops.stopFlows(others, job);
            if (stopAnalytics) {
                job.log("stopping the analytics services (they only consume snapshots and serve queries)");
                List<String> args = new ArrayList<>(List.of("stop"));
                args.addAll(ANALYTICS);
                ops.compose().compose(ANALYTICS_PROFILES, args, job::log, 300);
            }
            ops.bringUp(f, job, next.coordinators, next.workers, tuning);
            waitForInstances(f, next, job);
            job.log("load: " + next.rate + " instances/s on " + m + " for " + next.durationSec + " s");
            generator.start(m, next.rate, next.durationSec, false, !lockMode.equals("serial"));
            next.loadStartedAt = System.currentTimeMillis();
            next.mark("LOAD", m, next.rate + "/s for " + next.durationSec + " s" + (lockMode.equals("serial") ? "" : ", entity key only"));
            Thread.ofVirtual().name("perf-sampler").start(() -> sampleLoop(next));
            if (next.chaosOn) {
                startChaos(next);
                job.log("chaos: a random victim every " + next.chaosEverySec + " s" + (next.chaosKafka ? " (Kafka included)" : ""));
            }
        });
    }

    /** Stops the load and chaos and brings back what is down; the flow keeps running. */
    Map<String, Object> stop() {
        return ops.submit(JOB, "stop", job -> endRun(job, "stopped"));
    }

    private void endRun(FlowOps.Job job, String status) {
        Run r = run;
        if (r == null || !"running".equals(r.status)) {
            return;
        }
        r.status = status;
        r.chaosOn = false;
        Thread t = chaosThread;
        if (t != null) {
            t.interrupt();
        }
        generator.stop(r.machine);
        restore(r, job);
        r.mark(status.equals("completed") ? "END" : "STOP", r.flow, status.equals("completed") ? "load done and drained" : "load and chaos stopped");
        if (store != null) {
            try {
                store.endRun(r.runId, System.currentTimeMillis(), status);
            } catch (java.sql.SQLException | RuntimeException e) {
                job.log("run end not recorded: " + e.getMessage());
            }
        }
        job.log((status.equals("completed") ? "completed" : "stopped") + " the run " + r.runId);
    }

    /** Starts every stopped replica of the run's flow, and Kafka if this run stopped it. */
    Map<String, Object> restore() {
        Run r = requireRun();
        return ops.submit(JOB, "restore", job -> restore(r, job));
    }

    private void restore(Run r, FlowOps.Job job) {
        FlowRegistry.Flow f = ops.registry().get(r.flow);
        for (Map<String, Object> i : instances(f)) {
            if (!"running".equals(i.get("state"))) {
                String name = (String) i.get("name");
                job.log("starting " + name);
                ops.compose().docker(List.of("start", name), job::log, 60);
                r.mark("START", name, "restored");
            }
        }
        if (r.kafkaStoppedByUs) {
            job.log("starting kafka");
            ops.compose().compose(List.of("analytics"), List.of("start", "kafka"), job::log, 120);
            r.kafkaStoppedByUs = false;
            r.mark("START", "kafka", "restored");
        }
    }

    private void waitForInstances(FlowRegistry.Flow f, Run r, FlowOps.Job job) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 120_000;
        int want = r.coordinators + r.workers * f.workers().size();
        while (System.currentTimeMillis() < deadline) {
            long up = instances(f).stream().filter(i -> "running".equals(i.get("state"))).count();
            if (up >= want) {
                job.log(up + " instance(s) running");
                return;
            }
            Thread.sleep(1000);
        }
        job.log("not every instance is running after 120 s; continuing");
    }

    // ---------------------------------------------------------------- manual actions

    /** {@code kill} (SIGKILL), {@code stop} (SIGTERM, graceful) or {@code start} of one replica of the run's flow. */
    Map<String, Object> instance(String name, String op) {
        Run r = requireRun();
        FlowRegistry.Flow f = ops.registry().get(r.flow);
        if (instances(f).stream().noneMatch(i -> name.equals(i.get("name")))) {
            throw new IllegalArgumentException(name + " is not an instance of " + r.flow);
        }
        List<String> args = switch (op) {
            case "kill" -> List.of("kill", name);
            case "stop" -> List.of("stop", "--time", "10", name);
            case "start" -> List.of("start", name);
            default -> throw new IllegalArgumentException("unknown instance operation " + op + " (kill, stop, start)");
        };
        List<String> out = new ArrayList<>();
        int rc = ops.compose().docker(args, out::add, 60);
        if (rc != 0) {
            throw new IllegalStateException("docker " + String.join(" ", args) + ": " + String.join(" ", out));
        }
        r.mark(op.toUpperCase(java.util.Locale.ROOT), name, "manual");
        return view();
    }

    /** {@code stop} or {@code start} Kafka (the snapshot topics): workers keep transitioning, snapshots queue up. */
    Map<String, Object> kafka(String op) {
        Run r = requireRun();
        if (!op.equals("stop") && !op.equals("start")) {
            throw new IllegalArgumentException("unknown kafka operation " + op + " (stop, start)");
        }
        List<String> out = new ArrayList<>();
        int rc = ops.compose().compose(List.of("analytics"), List.of(op, "kafka"), out::add, 120);
        if (rc != 0) {
            throw new IllegalStateException("docker compose " + op + " kafka: " + String.join(" ", out));
        }
        r.kafkaStoppedByUs = op.equals("stop");
        r.mark(op.toUpperCase(java.util.Locale.ROOT), "kafka", "manual");
        return view();
    }

    /** Turns chaos on or off for the current run. */
    Map<String, Object> chaos(boolean on) {
        Run r = requireRun();
        if (on && !r.chaosOn) {
            r.chaosOn = true;
            startChaos(r);
            r.mark("CHAOS", r.flow, "on, every " + r.chaosEverySec + " s");
            if (store != null) {
                try {
                    store.setChaos(r.runId, true);
                } catch (java.sql.SQLException | RuntimeException e) {
                    log.warn("chaos flag of {} not stored: {}", r.runId, e.getMessage());
                }
            }
        } else if (!on && r.chaosOn) {
            r.chaosOn = false;
            Thread t = chaosThread;
            if (t != null) {
                t.interrupt();
            }
            r.mark("CHAOS", r.flow, "off");
        }
        return view();
    }

    private Run requireRun() {
        Run r = run;
        if (r == null) {
            throw new IllegalStateException("no performance run: start one first");
        }
        return r;
    }

    // ---------------------------------------------------------------- samples and history

    /**
     * Every {@link #SAMPLE_SEC} seconds while the run is current: the machine's metrics over the last window, the number
     * of running instances and Kafka's state, stored as one sample. {@link #DRAIN_SEC} seconds after the load ends the
     * run completes (chaos stops, whatever is down comes back).
     */
    private void sampleLoop(Run r) {
        FlowRegistry.Flow f = ops.registry().get(r.flow);
        long next = r.loadStartedAt + SAMPLE_SEC * 1000L;
        long loadEndedAt = 0;
        try {
            while (run == r && "running".equals(r.status)) {
                long wait = next - System.currentTimeMillis();
                if (wait > 0) {
                    Thread.sleep(wait);
                }
                next += SAMPLE_SEC * 1000L;
                if (run != r || !"running".equals(r.status)) {
                    return;
                }
                boolean loading = generator.status(r.machine).get("run") instanceof Map<?, ?> g && Boolean.TRUE.equals(g.get("running"));
                if (!loading && loadEndedAt == 0) {
                    loadEndedAt = System.currentTimeMillis();
                }
                record(r, f, loading);
                if (!loading && System.currentTimeMillis() - loadEndedAt >= DRAIN_SEC * 1000L) {
                    ops.submit(JOB, "complete", job -> endRun(job, "completed"));
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.warn("perf sampler of {} ended: {}", r.runId, e.getMessage());
        }
    }

    private void record(Run r, FlowRegistry.Flow f, boolean loading) {
        if (store == null) {
            return;
        }
        try {
            Map<String, Object> v = metrics.flowView(r.machine, SAMPLE_SEC);
            Map<?, ?> mach = (Map<?, ?>) v.getOrDefault("machine", Map.of());
            Map<?, ?> pub = (Map<?, ?>) v.getOrDefault("publisher", Map.of());
            Map<?, ?> coord = (Map<?, ?>) v.getOrDefault("coordinator", Map.of());
            Map<?, ?> work = (Map<?, ?>) v.getOrDefault("worker", Map.of());
            int up = (int) instances(f).stream().filter(i -> "running".equals(i.get("state"))).count();
            boolean kafkaUp = "running".equals(ops.containersOrEmptyFresh().getOrDefault("kafka", Map.of()).get("state"));
            long now = System.currentTimeMillis();
            store.addSample(r.runId, new PerfStore.Sample(now, (int) ((now - r.loadStartedAt) / 1000), loading,
                    dbl(pub.get("perSec")), dbl(coord.get("perSec")), dbl(work.get("perSec")), dbl(mach.get("completedPerSec")),
                    lng(mach.get("completed")), lng(work.get("errored")), lng(work.get("rejected")), dbl(mach.get("latencyP50")),
                    dbl(mach.get("latencyP95")), dbl(mach.get("latencyP99")), dbl(mach.get("lifetimeP50")), dbl(mach.get("lifetimeP95")),
                    lng(coord.get("lockWaits")), up, kafkaUp));
        } catch (java.sql.SQLException | RuntimeException e) {
            log.warn("perf sample of {} not stored: {}", r.runId, e.getMessage());
        }
    }

    private static Double dbl(Object o) {
        return o instanceof Number n ? n.doubleValue() : null;
    }

    private static Long lng(Object o) {
        return o instanceof Number n ? n.longValue() : null;
    }

    /** Past runs with their summaries, newest first. */
    Map<String, Object> history(int limit) {
        requireStore();
        try {
            return Map.of("runs", store.runs(Math.max(1, Math.min(limit, 500))), "store", store.file().toString());
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("reading past runs: " + e.getMessage(), e);
        }
    }

    /** Up to four runs side by side: settings, summary, samples and timeline. */
    Map<String, Object> compare(List<String> runIds) {
        requireStore();
        if (runIds.isEmpty() || runIds.size() > 4) {
            throw new IllegalArgumentException("compare 1 to 4 runs, got " + runIds.size());
        }
        try {
            return Map.of("runs", store.compare(runIds));
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("reading runs: " + e.getMessage(), e);
        }
    }

    Map<String, Object> deleteRun(String runId) {
        requireStore();
        Run r = run;
        if (r != null && r.runId.equals(runId) && "running".equals(r.status)) {
            throw new IllegalStateException(runId + " is running: stop it first");
        }
        try {
            return Map.of("deleted", store.delete(runId));
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("deleting " + runId + ": " + e.getMessage(), e);
        }
    }

    private void requireStore() {
        if (store == null) {
            throw new IllegalStateException("past runs are not kept: the DuckDB file (PERF_DB) could not be opened");
        }
    }

    // ---------------------------------------------------------------- chaos

    private synchronized void startChaos(Run r) {
        Thread old = chaosThread;
        if (old != null && old.isAlive()) {
            return;
        }
        chaosThread = Thread.ofVirtual().name("perf-chaos").start(() -> chaosLoop(r));
    }

    /**
     * One victim at a time: a coordinator or worker replica killed (SIGKILL) or stopped gracefully, or Kafka stopped,
     * kept down 3 to 12 s, then started again. The flow's last running replica of a role can go down too: the work
     * waits in Temporal and Kinesis until it is back, which is part of what a run shows.
     */
    private void chaosLoop(Run r) {
        FlowRegistry.Flow f = ops.registry().get(r.flow);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        try {
            while (r.chaosOn && run == r) {
                Thread.sleep(r.chaosEverySec * 1000L);
                if (!r.chaosOn || run != r) {
                    return;
                }
                int roll = rnd.nextInt(100);
                long down = 3000 + rnd.nextLong(9001);
                if (r.chaosKafka && roll < 15) {
                    List<String> sink = new ArrayList<>();
                    ops.compose().compose(List.of("analytics"), List.of("stop", "kafka"), sink::add, 120);
                    r.kafkaStoppedByUs = true;
                    r.mark("CHAOS STOP", "kafka", "down " + down / 1000 + " s");
                    try {
                        Thread.sleep(down);
                    } finally { // also when chaos is turned off meanwhile: the victim always comes back
                        ops.compose().compose(List.of("analytics"), List.of("start", "kafka"), sink::add, 120);
                        r.kafkaStoppedByUs = false;
                        r.mark("START", "kafka", "after chaos");
                    }
                    continue;
                }
                String role = roll < 60 ? "coordinator" : "worker";
                boolean hard = rnd.nextInt(100) < 70;
                List<Map<String, Object>> candidates = instances(f).stream()
                        .filter(i -> role.equals(i.get("role")) && "running".equals(i.get("state"))).toList();
                if (candidates.isEmpty()) {
                    continue;
                }
                String victim = (String) candidates.get(rnd.nextInt(candidates.size())).get("name");
                List<String> sink = new ArrayList<>();
                ops.compose().docker(hard ? List.of("kill", victim) : List.of("stop", "--time", "10", victim), sink::add, 60);
                r.mark(hard ? "CHAOS KILL" : "CHAOS STOP", victim, (hard ? "SIGKILL" : "SIGTERM") + ", down " + down / 1000 + " s");
                try {
                    Thread.sleep(down);
                } finally { // also when chaos is turned off meanwhile: the victim always comes back
                    ops.compose().docker(List.of("start", victim), sink::add, 60);
                    r.mark("START", victim, "after chaos");
                }
            }
        } catch (InterruptedException e) {
            // chaos turned off or the run ended
        } catch (RuntimeException e) {
            log.warn("chaos loop of {} ended: {}", r.flow, e.getMessage());
            r.mark("CHAOS", r.flow, "stopped: " + e.getMessage());
        }
    }
}
