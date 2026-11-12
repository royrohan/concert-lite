package io.concert.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.TaskQueues;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EntityInit;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.TransitionResult;
import io.concert.store.InMemoryStateStore;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.client.UpdateOptions;
import io.temporal.client.WithStartWorkflowOperation;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowUpdateStage;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractStateMachineTest {

    public static class DoorMachine extends AbstractStateMachine {
        static final StateMachineSpec SPEC = StateMachineSpec.startingAt("CLOSED")
                .on("CLOSED", "open", "OPEN")
                .on("OPEN", "close", "CLOSED")
                .build();

        @Override
        protected StateMachineSpec spec() {
            return SPEC;
        }

        @Override
        protected int continueAsNewAfter() {
            return 3; // exercise continue-as-new in a short test
        }
    }

    private TestWorkflowEnvironment env;
    private InMemoryStateStore store;
    private WorkflowClient client;

    @BeforeEach
    void setUp() {
        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "false");
        env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setUseTimeskipping(false).build());
        store = new InMemoryStateStore();
        Worker w = env.newWorker(TaskQueues.stateMachine("door"));
        w.registerWorkflowImplementationTypes(DoorMachine.class);
        w.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store));
        env.start();
        client = env.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        env.close();
    }

    private TransitionResult send(String id, String type) {
        EntityWorkflow wf = client.newWorkflowStub(EntityWorkflow.class, WorkflowOptions.newBuilder()
                .setWorkflowId(WorkflowIds.entity("door", "d1"))
                .setTaskQueue(TaskQueues.stateMachine("door"))
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                .build());
        EventEnvelope e = new EventEnvelope(id, "door", "d1", type, List.of(), null, 0, 0);
        return WorkflowClient.executeUpdateWithStart(wf::handle, e,
                UpdateOptions.<TransitionResult>newBuilder().setUpdateId(id).setWaitForStage(WorkflowUpdateStage.COMPLETED).build(),
                new WithStartWorkflowOperation<>(wf::run, EntityInit.fresh("door", "d1")));
    }

    @Test
    void appliesTransitionsInOrderAndSurvivesContinueAsNew() {
        assertEquals("OPEN", send("e1", "open").toState());
        assertEquals("CLOSED", send("e2", "close").toState());
        TransitionResult rejected = send("e3", "close");
        assertFalse(rejected.accepted());
        // continue-as-new happened after 3 events; state and version carry over
        TransitionResult r = send("e4", "open");
        assertTrue(r.accepted());
        assertEquals("OPEN", r.toState());
        assertEquals(3, r.version());
        assertEquals("OPEN", store.loadState("door:d1").orElseThrow().state());
        assertEquals(3, store.loadState("door:d1").orElseThrow().version());
    }

    @Test
    void retriedUpdateIdIsIdempotent() {
        TransitionResult first = send("same", "open");
        TransitionResult again = send("same", "open");
        assertEquals(first, again);
        assertEquals(1, store.loadState("door:d1").orElseThrow().version());
    }

    // ------------------------------------------------------------------ DB-truth

    @Test
    void anEventRedeliveredAfterContinueAsNewIsAppliedOnce() {
        TransitionResult first = send("e1", "open");
        send("e2", "close");
        send("e3", "open"); // continue-as-new after 3: a new run, which has never seen update id e1

        TransitionResult again = send("e1", "open");

        assertEquals(first, again, "the stored result of the first application");
        assertEquals(3, store.loadState("door:d1").orElseThrow().version(), "not applied a second time");
        assertEquals("CLOSED", send("e4", "close").toState(), "the run carries on from the stored state");
    }

    @Test
    void aStaleCopyIsDetectedAtCommitAndTheEventDecidedAgainOnTheStoredEntity() {
        assertEquals("OPEN", send("e1", "open").toState());
        // someone else moves the entity in the database (the workflow's copy is now stale)
        store.commitTransition(new io.concert.common.api.TransitionCommit("door:d1", "x1", 1,
                new TransitionResult("x1", "OPEN", "CLOSED", true, null, 2),
                new io.concert.common.api.SmStateRow("door:d1", "door", "CLOSED", null, 2, "x1", 0), List.of(), 0));

        TransitionResult r = send("e2", "open"); // on the stale copy (OPEN) this would be rejected

        assertTrue(r.accepted(), r.toString());
        assertEquals("CLOSED", r.fromState());
        assertEquals(3, r.version());
    }

    @Test
    void whileTheStoreIsDownNothingIsCommittedAndTheEventWaitsForIt() {
        tearDown();
        FlakyStore flaky = new FlakyStore(new InMemoryStateStore(), 3);
        store = flaky.memory;
        env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setUseTimeskipping(false).build());
        Worker w = env.newWorker(TaskQueues.stateMachine("door"));
        w.registerWorkflowImplementationTypes(DoorMachine.class);
        w.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(flaky.proxy()));
        env.start();
        client = env.getWorkflowClient();

        TransitionResult r = send("e1", "open"); // the first three commits fail; the local activity retries

        assertTrue(r.accepted());
        assertEquals(1, r.version());
        assertEquals(4, flaky.commits.get(), "three failed attempts, then the commit");
        assertEquals(1, store.loadState("door:d1").orElseThrow().version());
    }

    /** A store whose first {@code failures} commits throw, as if the database were unreachable. */
    static final class FlakyStore {
        final InMemoryStateStore memory;
        final java.util.concurrent.atomic.AtomicInteger commits = new java.util.concurrent.atomic.AtomicInteger();
        private final int failures;

        FlakyStore(InMemoryStateStore memory, int failures) {
            this.memory = memory;
            this.failures = failures;
        }

        io.concert.store.StateStore proxy() {
            return (io.concert.store.StateStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {io.concert.store.StateStore.class}, (proxy, method, args) -> {
                        if (method.getName().equals("commitTransition") && commits.incrementAndGet() <= failures) {
                            throw new IllegalStateException("database unreachable");
                        }
                        try {
                            return method.invoke(memory, args);
                        } catch (java.lang.reflect.InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }
    }

    @Test
    void mermaidHighlightsCurrentState() {
        String m = DoorMachine.SPEC.toMermaid("OPEN");
        assertTrue(m.contains("CLOSED --> OPEN : open"));
        assertTrue(m.contains("class OPEN current"));
    }
}
