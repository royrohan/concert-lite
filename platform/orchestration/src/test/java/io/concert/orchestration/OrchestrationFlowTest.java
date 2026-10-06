package io.concert.orchestration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.common.TaskQueues;
import io.concert.common.TraceRow;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EntitySnapshot;
import io.concert.common.api.EntityWorkflow;
import io.concert.sdk.AbstractStateMachine;
import io.concert.sdk.EntityPersistenceActivitiesImpl;
import io.concert.sdk.StateMachineSpec;
import io.concert.store.InMemoryStateStore;
import io.concert.store.TraceWriter;
import io.temporal.client.WorkflowClient;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Lock / event workflows end to end on Temporal's in-memory test server (no Docker needed). */
class OrchestrationFlowTest {

    /** Counts appends and how many arrived with a lower seq than one already seen. */
    public static class Ledger extends AbstractStateMachine {
        static final StateMachineSpec SPEC = StateMachineSpec.startingAt("OPEN").on("OPEN", "append", "OPEN").build();

        @Override
        protected StateMachineSpec spec() {
            return SPEC;
        }

        @Override
        protected String applyData(String from, String to, String data, EventEnvelope e) {
            long count = 0, last = -1, ooo = 0;
            if (data != null) {
                JsonNode d = Json.read(data, JsonNode.class);
                count = d.get("count").asLong();
                last = d.get("last").asLong();
                ooo = d.get("ooo").asLong();
            }
            long seq = Json.read(e.payload(), JsonNode.class).get("seq").asLong();
            return "{\"count\":" + (count + 1) + ",\"last\":" + Math.max(seq, last) + ",\"ooo\":" + (ooo + (seq < last ? 1 : 0)) + "}";
        }
    }

    private TestWorkflowEnvironment env;
    private InMemoryStateStore store;
    private TraceWriter traces;
    private IngestDispatcher dispatcher;
    private WorkflowClient client;

    @BeforeEach
    void setUp() {
        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "false");
        OverlapProbe.reset();
        env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setUseTimeskipping(false).build());
        client = env.getWorkflowClient();
        store = new InMemoryStateStore();
        traces = new TraceWriter(store, 100_000, 500, 10);
        DispatchActivitiesImpl dispatch = new DispatchActivitiesImpl(client, traces);

        Worker orch = env.newWorker(TaskQueues.ORCHESTRATION);
        orch.registerWorkflowImplementationTypes(KeyLockWorkflowImpl.class);
        orch.registerActivitiesImplementations(dispatch);

        Worker ledger = env.newWorker(TaskQueues.stateMachine("ledger"));
        ledger.registerWorkflowImplementationTypes(Ledger.class);
        ledger.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store));

        env.start();
        dispatcher = new IngestDispatcher(store, traces, dispatch, Set.of(), 64);
    }

    @AfterEach
    void tearDown() {
        dispatcher.close();
        traces.close();
        env.close();
    }

    private static EventEnvelope append(String id, String ledger, long seq, List<String> extraKeys) {
        return new EventEnvelope(id, "ledger", ledger, "append", extraKeys, "{\"seq\":" + seq + "}",
                System.currentTimeMillis(), System.currentTimeMillis());
    }

    private JsonNode ledgerData(String ledger) {
        EntitySnapshot s = client.newWorkflowStub(EntityWorkflow.class, WorkflowIds.entity("ledger", ledger)).snapshot();
        return s.data() == null ? Json.read("{\"count\":0,\"last\":-1,\"ooo\":0}", JsonNode.class) : Json.read(s.data(), JsonNode.class);
    }

    private static void await(Duration timeout, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                if (cond.getAsBoolean()) {
                    return;
                }
            } catch (RuntimeException notYet) {
                // workflow not started yet
            }
            Thread.sleep(50);
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    @Test
    void singleKeyEventsArriveInShardOrder() throws Exception {
        int n = 200;
        for (int b = 0; b < n; b += 50) {
            List<EventEnvelope> batch = new ArrayList<>();
            for (int i = b; i < b + 50; i++) {
                batch.add(append("s-" + i, "L1", i, List.of()));
            }
            dispatcher.dispatchBatch(batch);
        }
        await(Duration.ofSeconds(60), () -> ledgerData("L1").get("count").asLong() == n);
        assertEquals(0, ledgerData("L1").get("ooo").asLong());
        assertEquals(n - 1, ledgerData("L1").get("last").asLong());
    }

    @Test
    void duplicatesAreDroppedUsingTheStore() throws Exception {
        EventEnvelope e = append("dup-1", "L2", 1, List.of());
        dispatcher.dispatchBatch(List.of(e, e));
        dispatcher.dispatchBatch(List.of(e));
        await(Duration.ofSeconds(30), () -> ledgerData("L2").get("count").asLong() == 1);
        Thread.sleep(500);
        assertEquals(1, ledgerData("L2").get("count").asLong());
        assertEquals("DISPATCHED", store.processedStatus("dup-1").orElseThrow());
        assertEquals(2, dispatcher.droppedCount());
        await(Duration.ofSeconds(5), () -> store.traceForEvent("dup-1").stream()
                .filter(t -> t.stage() == TraceRow.Stage.DROPPED).count() == 2);
    }

    @Test
    void multiKeyEventsNeverOverlapAndNeverDeadlock() throws Exception {
        Random rnd = new Random(42);
        int n = 300;
        String[] accounts = {"account:A", "account:B", "account:C"};
        long[] perLedger = new long[10];
        List<EventEnvelope> batch = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int ledger = rnd.nextInt(perLedger.length);
            List<String> keys = new ArrayList<>();
            int extra = rnd.nextInt(3); // 0 = single-key (inline path), 1-2 = multi-key
            for (int k = 0; k < extra; k++) {
                keys.add(accounts[rnd.nextInt(accounts.length)]);
            }
            batch.add(append("m-" + i, "L" + ledger, perLedger[ledger]++, keys));
            if (batch.size() == 50) {
                dispatcher.dispatchBatch(batch);
                batch = new ArrayList<>();
            }
        }
        dispatcher.dispatchBatch(batch);

        await(Duration.ofSeconds(120), () -> {
            long total = 0;
            for (int l = 0; l < perLedger.length; l++) {
                total += ledgerData("L" + l).get("count").asLong();
            }
            return total == n;
        });
        assertEquals(0, OverlapProbe.violations(), "events sharing a key were applied concurrently");
        for (int l = 0; l < perLedger.length; l++) {
            assertEquals(perLedger[l], ledgerData("L" + l).get("count").asLong());
        }
        // Every multi-key event leaves a full trace: one LOCK_GRANTED per key, then DONE.
        await(Duration.ofSeconds(5), () -> store.traceForEvent("m-0").stream().anyMatch(t -> t.stage() == TraceRow.Stage.DONE));
        assertTrue(store.traceForEvent("m-0").stream().anyMatch(t -> t.stage() == TraceRow.Stage.LOCK_GRANTED));
    }
}
