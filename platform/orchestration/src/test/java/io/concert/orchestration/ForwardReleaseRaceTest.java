package io.concert.orchestration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.concert.common.EventEnvelope;
import io.concert.common.TaskQueues;
import io.concert.common.TraceRow;
import io.concert.common.WorkflowIds;
import io.concert.common.api.DispatchActivities;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.LockRequest;
import io.concert.common.api.TransitionResult;
import io.concert.sdk.EntityPersistenceActivitiesImpl;
import io.concert.store.InMemoryStateStore;
import io.concert.store.TraceWriter;
import io.temporal.client.WorkflowClient;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A forward that takes longer than the lock's workflow task (the local activity heartbeats the task, so
 * updates are handled meanwhile) while the chain already finished and released the head: the next head
 * must still be forwarded, not parked as "forwarded" until the one-hour lease (seen live with the
 * trading flows, where enqueues are slow under load).
 */
class ForwardReleaseRaceTest {

    /** Real dispatch, but every forward to a later key returns 3 s after the enqueue succeeded. */
    static final class SlowForwards implements DispatchActivities {
        private final DispatchActivitiesImpl real;

        SlowForwards(DispatchActivitiesImpl real) {
            this.real = real;
        }

        @Override
        public TransitionResult dispatchToEntity(EventEnvelope event, Map<String, Long> lockWaitMs) {
            return real.dispatchToEntity(event, lockWaitMs);
        }

        @Override
        public void enqueueLock(String lockKey, LockRequest request) {
            real.enqueueLock(lockKey, request);
            if (request.keyIndex() > 0) {
                try {
                    Thread.sleep(3000); // > the 2 s workflow task timeout: the task is heartbeated meanwhile
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void releaseLocks(List<String> lockKeys, String requestId) {
            real.releaseLocks(lockKeys, requestId);
        }

        @Override
        public void recordTrace(List<TraceRow> rows) {
            real.recordTrace(rows);
        }
    }

    private TestWorkflowEnvironment env;
    private TraceWriter traces;
    private IngestDispatcher dispatcher;
    private WorkflowClient client;

    @BeforeEach
    void setUp() {
        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "false");
        env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setUseTimeskipping(false).build());
        client = env.getWorkflowClient();
        InMemoryStateStore store = new InMemoryStateStore();
        traces = new TraceWriter(store, 100_000, 500, 10);
        DispatchActivitiesImpl real = new DispatchActivitiesImpl(client, traces);
        Worker orch = env.newWorker(TaskQueues.ORCHESTRATION);
        orch.registerWorkflowImplementationTypes(KeyLockWorkflowImpl.class);
        orch.registerActivitiesImplementations(new SlowForwards(real));
        Worker ledger = env.newWorker(TaskQueues.stateMachine("ledger"));
        ledger.registerWorkflowImplementationTypes(OrchestrationFlowTest.Ledger.class);
        ledger.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store));
        env.start();
        dispatcher = new IngestDispatcher(store, traces, real, Set.of(), 64);
    }

    @AfterEach
    void tearDown() {
        dispatcher.close();
        traces.close();
        env.close();
    }

    @Test
    void nextHeadIsForwardedWhenTheHeadWasReleasedDuringItsForward() throws Exception {
        List<EventEnvelope> batch = List.of(
                new EventEnvelope("r-1", "ledger", "R", "append", List.of("account:A"), "{\"seq\":0}", 0, 0),
                new EventEnvelope("r-2", "ledger", "R", "append", List.of("account:A"), "{\"seq\":1}", 0, 0));
        dispatcher.dispatchBatch(batch);
        long deadline = System.currentTimeMillis() + 30_000;
        long count = 0;
        while (System.currentTimeMillis() < deadline && count < 2) {
            try {
                String data = client.newWorkflowStub(EntityWorkflow.class, WorkflowIds.entity("ledger", "R")).snapshot().data();
                count = data == null ? 0 : io.concert.common.Json.read(data, com.fasterxml.jackson.databind.JsonNode.class)
                        .get("count").asLong();
            } catch (RuntimeException notYet) {
                // entity not started yet
            }
            Thread.sleep(100);
        }
        assertEquals(2, count, "second event parked behind a stale forward");
    }
}
