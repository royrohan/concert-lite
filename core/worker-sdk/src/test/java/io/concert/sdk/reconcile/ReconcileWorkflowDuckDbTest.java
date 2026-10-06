package io.concert.sdk.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.concert.common.TaskQueues;
import io.concert.common.api.SmStateRow;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.sink.SchemaMapper;
import io.concert.sink.SchemaSpec;
import io.concert.sink.SnapshotRecord;
import io.concert.sink.duckdb.DuckDbTarget;
import io.concert.sink.duckdb.ReadOnlyQueries;
import io.concert.sink.duckdb.SinkHttpServer;
import io.concert.store.InMemoryStateStore;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link ReconcileWorkflow} end to end without Kafka: Temporal test server, in-memory StateStore, a real
 * DuckDB sink (fresh file) behind its HTTP API ({@code POST /versions}), and a publisher that applies
 * snapshots to that DuckDB like the consumer loop would. Reconciliation restores missing and stale rows.
 */
class ReconcileWorkflowDuckDbTest {

    @TempDir
    Path dir;

    private TestWorkflowEnvironment env;
    private DuckDbTarget db;
    private ReadOnlyQueries queries;
    private SinkHttpServer http;
    private final InMemoryStateStore store = new InMemoryStateStore();

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "false");
        SchemaSpec schema = new SchemaMapper(SchemaMapper.loadModels(System.getProperty("concert.testModelsDir")))
                .map(SchemaMapper.parseRoots("ticket:demo::ticket::Ticket"));
        db = new DuckDbTarget(dir.resolve("fresh.duckdb").toString());
        db.ensureSchema(schema);
        queries = new ReadOnlyQueries(db, Duration.ofSeconds(10));
        http = new SinkHttpServer(db, queries, () -> null).start(0);

        AtomicLong offset = new AtomicLong();
        EntitySnapshotPublisher toDuckDb = s -> db.apply(List.of(new SnapshotRecord(s, "entity-snapshots", 0, offset.getAndIncrement())));
        env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setUseTimeskipping(false).build());
        Worker w = env.newWorker(TaskQueues.stateMachine("ticket"));
        w.registerWorkflowImplementationTypes(ReconcileWorkflowImpl.class);
        w.registerActivitiesImplementations(new ReconcileActivitiesImpl("ticket",
                new Reconciler(store, ReconcilerTest.SPEC, new DuckDbSinkVersions("http://localhost:" + http.port()), toDuckDb),
                null));
        env.start();

        for (int i = 1; i <= 3; i++) {
            store.upsertState(new SmStateRow("ticket:t" + i, "ticket", "CLOSED",
                    "{\"id\":\"t" + i + "\",\"priority\":\"HIGH\",\"reopenCount\":" + i + "}", 4, "e" + i, 1_759_600_000_000L), null);
        }
        store.upsertState(new SmStateRow("ticket:t4", "ticket", "OPEN", "{\"id\":\"t4\"}", 1, "e4", 1_759_600_000_000L), null);
        // The sink already has t1 at the current version and t2 at an older one; t3 is missing.
        toDuckDb.publish(Reconciler.snapshot(store.loadState("ticket:t1").orElseThrow()));
        toDuckDb.publish(Reconciler.snapshot(new SmStateRow("ticket:t2", "ticket", "CLOSED", "{\"id\":\"t2\"}", 2, "e", 1)));
    }

    @AfterEach
    void tearDown() {
        env.close();
        http.close();
        queries.close();
        db.close();
    }

    private ReconcileReport reconcile(boolean dryRun) {
        return env.getWorkflowClient().newWorkflowStub(ReconcileWorkflow.class, WorkflowOptions.newBuilder()
                .setWorkflowId("reconcile-ticket-" + System.nanoTime()).setTaskQueue(TaskQueues.stateMachine("ticket")).build())
                .run(new ReconcileRequest("ticket", dryRun, 2));
    }

    private Object scalar(String sql) throws Exception {
        return queries.run(sql).rows().getFirst().getFirst();
    }

    @Test
    void restoresMissingAndStaleRows() throws Exception {
        ReconcileReport dry = reconcile(true);
        assertEquals(4, dry.scanned());
        assertEquals(3, dry.terminal());
        assertEquals(1, dry.missing());
        assertEquals(1, dry.stale());
        assertEquals(0, dry.republished());
        assertEquals(1L, ((Number) scalar("SELECT count(*) FROM tickets WHERE entity_id = 'ticket:t3' OR entity_version = 4")).longValue());

        ReconcileReport r = reconcile(false);
        assertEquals(2, r.republished());
        assertEquals(3L, ((Number) scalar("SELECT count(*) FROM tickets WHERE entity_version = 4")).longValue());
        assertEquals(3L, ((Number) scalar("SELECT reopen_count FROM tickets WHERE entity_id = 'ticket:t3'")).longValue());

        ReconcileReport again = reconcile(false);
        assertEquals(0, again.missing() + again.stale() + again.republished(), "nothing left to do");
    }
}
