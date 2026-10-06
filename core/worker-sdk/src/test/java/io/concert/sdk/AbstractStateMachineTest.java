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

    @Test
    void mermaidHighlightsCurrentState() {
        String m = DoorMachine.SPEC.toMermaid("OPEN");
        assertTrue(m.contains("CLOSED --> OPEN : open"));
        assertTrue(m.contains("class OPEN current"));
    }
}
