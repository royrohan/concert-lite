package io.concert.it;

import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.common.api.IngestConfig;
import io.concert.orchestration.OrchestrationWorker;
import io.concert.samples.SampleMachines;
import io.concert.samples.SimulatedWork;
import io.concert.samples.events.DemoEvents;
import io.concert.sdk.WorkerBootstrap;
import io.concert.sdk.events.EventWorkerBootstrap;
import io.concert.store.JdbcStateStore;
import io.temporal.client.WorkflowClient;
import io.temporal.worker.WorkerFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.HdrHistogram.Histogram;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

/** Wires a full pipeline against the shared containers: stream, coordinators, typed workers. */
final class Harness implements AutoCloseable {

    final String stream;
    final KinesisClient kinesis;
    final JdbcStateStore store;
    private final List<OrchestrationWorker> coordinators = new ArrayList<>();
    private final List<WorkerFactory> workers = new ArrayList<>();
    private final List<WorkflowClient> clients = new ArrayList<>();

    Harness(int shards) {
        Stack.start();
        this.stream = "it-" + UUID.randomUUID().toString().substring(0, 8);
        this.kinesis = Stack.kinesis();
        this.store = (JdbcStateStore) Stack.store(); // benchmarks use Postgres-specific SQL (percentiles)
        kinesis.createStream(r -> r.streamName(stream).shardCount(shards));
        await(Duration.ofSeconds(60), () -> kinesis.describeStreamSummary(r -> r.streamName(stream))
                .streamDescriptionSummary().streamStatus() == StreamStatus.ACTIVE);
    }

    OrchestrationWorker startCoordinator() {
        WorkflowClient client = newClient();
        OrchestrationWorker w = new OrchestrationWorker(client, store, kinesis, OrchestrationWorker.Settings.defaults()).start();
        coordinators.add(w);
        return w;
    }

    void startIngest() {
        coordinators.get(0).ensureIngest(new IngestConfig(stream, "TRIM_HORIZON", 2, 30, List.of(), 3));
    }

    /** One "worker deployment" for a type: its own client connection and worker factory. */
    void startWorker(String smType) {
        startWorker(smType, 2000);
    }

    /** @param localActivitySlots concurrent transitions this worker can run (its capacity with workMs) */
    void startWorker(String smType, int localActivitySlots) {
        workers.add(WorkerBootstrap.startWithSlots(newClient(), store, smType, SampleMachines.get(smType).impl(),
                localActivitySlots, new SimulatedWork.Impl()));
    }

    /** The event-style demo domain's worker (task queue ev-demo). */
    void startEventWorker() {
        workers.add(EventWorkerBootstrap.start(newClient(), store, DemoEvents.handlers(), EventWorkerBootstrap.Options.defaults()));
    }

    void stopCoordinator(OrchestrationWorker w) {
        w.close();
        coordinators.remove(w);
    }

    private WorkflowClient newClient() {
        WorkflowClient c = Stack.newClient();
        clients.add(c);
        return c;
    }

    /** Puts events with partition key = first lock key, so a key always lands on one shard. */
    void publish(List<EventEnvelope> events) {
        for (int from = 0; from < events.size(); from += 500) {
            List<PutRecordsRequestEntry> entries = events.subList(from, Math.min(events.size(), from + 500)).stream()
                    .map(e -> PutRecordsRequestEntry.builder()
                            .partitionKey(e.effectiveLockKeys().get(0))
                            .data(SdkBytes.fromByteArray(Json.write(e).getBytes()))
                            .build())
                    .toList();
            PutRecordsResponse resp = kinesis.putRecords(r -> r.streamName(stream).records(entries));
            if (resp.failedRecordCount() != null && resp.failedRecordCount() > 0) {
                throw new IllegalStateException(resp.failedRecordCount() + " records failed to publish");
            }
        }
    }

    static EventEnvelope event(String id, String smType, String instance, String type, List<String> keys, String payload) {
        return new EventEnvelope(id, smType, instance, type, keys, payload, System.currentTimeMillis(), 0);
    }

    // ---- assertions & stats straight from the DSQL stand-in ----

    long countTrace(String eventIdPrefix, String stage) {
        return queryLong("SELECT count(*) FROM event_trace WHERE stage = ? AND event_id LIKE ?", stage, eventIdPrefix + "%");
    }

    long countProcessed(String eventIdPrefix) {
        return queryLong("SELECT count(*) FROM processed_event WHERE event_id LIKE ?", eventIdPrefix + "%");
    }

    String stateData(String workflowId) {
        return store.loadState(workflowId).map(r -> r.data()).orElse(null);
    }

    record Stats(long completed, double throughputPerSec, Map<String, Long> perType, Histogram e2e, Histogram fromSource) {
        String format() {
            return String.format("completed=%d  throughput=%.0f/s  perType=%s  ingest->done p50=%dms p99=%dms max=%dms  "
                            + "producer->done p50=%dms p99=%dms",
                    completed, throughputPerSec, perType,
                    e2e.getValueAtPercentile(50), e2e.getValueAtPercentile(99), e2e.getMaxValue(),
                    fromSource.getValueAtPercentile(50), fromSource.getValueAtPercentile(99));
        }
    }

    private static final Pattern E2E = Pattern.compile("e2eMs=(-?\\d+) srcMs=(-?\\d+)");

    /** Throughput = completed / (last DONE - first RECEIVED) for events with the prefix. */
    Stats stats(String eventIdPrefix) {
        Histogram e2e = new Histogram(TimeUnit.MINUTES.toMillis(10), 3);
        Histogram src = new Histogram(TimeUnit.MINUTES.toMillis(10), 3);
        Map<String, Long> perType = new TreeMap<>();
        long completed = 0;
        try (Connection c = store.dataSource().getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT workflow_id, detail FROM event_trace WHERE stage = 'DONE' AND event_id LIKE ?")) {
            ps.setString(1, eventIdPrefix + "%");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    completed++;
                    String wf = rs.getString(1);
                    perType.merge(wf.substring(0, wf.indexOf(':')), 1L, Long::sum);
                    Matcher m = E2E.matcher(rs.getString(2));
                    if (m.find()) {
                        e2e.recordValue(Math.max(0, Long.parseLong(m.group(1))));
                        src.recordValue(Math.max(0, Long.parseLong(m.group(2))));
                    }
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        long first = queryLong("SELECT (extract(epoch FROM min(ts)) * 1000)::bigint FROM event_trace WHERE stage = 'RECEIVED' AND event_id LIKE ?", eventIdPrefix + "%");
        long last = queryLong("SELECT (extract(epoch FROM max(ts)) * 1000)::bigint FROM event_trace WHERE stage = 'DONE' AND event_id LIKE ?", eventIdPrefix + "%");
        double secs = Math.max(0.001, (last - first) / 1000.0);
        return new Stats(completed, completed / secs, perType, e2e, src);
    }

    /**
     * Per-event time in each pipeline section, p50/p99 (ms):
     * <ol>
     *   <li>kinesis: producer put -> record read by the shard consumer
     *   <li>dedupe: read -> DSQL dedupe done (batch parse + claim)
     *   <li>lock: dedupe done -> entity dispatch starts (acquire update, queueing, multi-key chain)
     *   <li>entity: dispatch -> transition running in the entity workflow (UpdateWithStart hop)
     *   <li>apply: transition -> done (onTransition work + DSQL projection + update response)
     * </ol>
     */
    String breakdown(String eventIdPrefix) {
        String sql = """
                WITH t AS (SELECT event_id, stage, min(ts) ts, min(detail) detail FROM event_trace
                           WHERE event_id LIKE ? GROUP BY event_id, stage),
                p AS (SELECT event_id,
                  max(substring(detail from 'kinesisMs=(-?[0-9]+)')::float) FILTER (WHERE stage='RECEIVED') kin,
                  max(substring(detail from 'dedupeMs=(-?[0-9]+)')::float) FILTER (WHERE stage='RECEIVED') ded,
                  max(ts) FILTER (WHERE stage='RECEIVED') rec, max(ts) FILTER (WHERE stage='LOCK_GRANTED') lg,
                  max(ts) FILTER (WHERE stage='TRANSITION') tr, max(ts) FILTER (WHERE stage='DONE') dn
                  FROM t GROUP BY event_id),
                d AS (SELECT kin, ded, extract(epoch FROM lg - rec) * 1000 lck, extract(epoch FROM tr - lg) * 1000 ent,
                  extract(epoch FROM dn - tr) * 1000 app, extract(epoch FROM dn - rec) * 1000 + ded tot FROM p WHERE dn IS NOT NULL)
                SELECT \s""";
        String[] cols = {"kin", "ded", "lck", "ent", "app", "tot"};
        String[] names = {"kinesis", "dedupe", "lock", "entity", "apply", "ingest->done"};
        StringBuilder q = new StringBuilder(sql);
        for (int i = 0; i < cols.length; i++) {
            q.append(i > 0 ? ", " : "")
                    .append("percentile_cont(0.5) WITHIN GROUP (ORDER BY ").append(cols[i]).append("), ")
                    .append("percentile_cont(0.99) WITHIN GROUP (ORDER BY ").append(cols[i]).append("), ")
                    .append("avg(").append(cols[i]).append(")");
        }
        q.append(" FROM d");
        try (Connection c = store.dataSource().getConnection(); PreparedStatement ps = c.prepareStatement(q.toString())) {
            ps.setString(1, eventIdPrefix + "%");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                StringBuilder out = new StringBuilder();
                for (int i = 0; i < cols.length; i++) {
                    out.append(String.format("%s p50=%.0f p99=%.0f avg=%.1f | ", names[i],
                            rs.getDouble(3 * i + 1), rs.getDouble(3 * i + 2), rs.getDouble(3 * i + 3)));
                }
                return out.toString();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /** ingest->done p50/p99 for events {@code <prefix><n>} with n below / at-or-above {@code split}. */
    String coldVsWarm(String prefix, int split) {
        Histogram cold = new Histogram(TimeUnit.MINUTES.toMillis(10), 3);
        Histogram warm = new Histogram(TimeUnit.MINUTES.toMillis(10), 3);
        try (Connection c = store.dataSource().getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT event_id, detail FROM event_trace WHERE stage = 'DONE' AND event_id LIKE ?")) {
            ps.setString(1, prefix + "%");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int n = Integer.parseInt(rs.getString(1).substring(prefix.length()));
                    Matcher m = E2E.matcher(rs.getString(2));
                    if (m.find()) {
                        (n < split ? cold : warm).recordValue(Math.max(0, Long.parseLong(m.group(1))));
                    }
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return String.format("first-event-per-key p50=%dms p99=%dms (n=%d) | warm p50=%dms p99=%dms (n=%d)",
                cold.getValueAtPercentile(50), cold.getValueAtPercentile(99), cold.getTotalCount(),
                warm.getValueAtPercentile(50), warm.getValueAtPercentile(99), warm.getTotalCount());
    }

    private long queryLong(String sql, String... args) {
        try (Connection c = store.dataSource().getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setString(i + 1, args[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static void await(Duration timeout, BooleanSupplier cond) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                if (cond.getAsBoolean()) {
                    return;
                }
            } catch (RuntimeException notYet) {
                // keep polling
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted");
            }
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    @Override
    public void close() {
        // The supervisor lives in Temporal, not in this JVM: stop it so later tests don't inherit it.
        try {
            Stack.newClient().newUntypedWorkflowStub(io.concert.common.WorkflowIds.ingest(stream)).terminate("test finished");
        } catch (RuntimeException ignored) {
            // never started
        }
        coordinators.forEach(OrchestrationWorker::close);
        workers.forEach(f -> {
            f.shutdownNow();
            f.awaitTermination(5, TimeUnit.SECONDS);
        });
        clients.forEach(c -> c.getWorkflowServiceStubs().shutdownNow());
        store.close();
        kinesis.close();
    }
}
