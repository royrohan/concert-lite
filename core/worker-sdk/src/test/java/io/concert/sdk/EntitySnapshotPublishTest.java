package io.concert.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.TaskQueues;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EntityInit;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.TransitionRecord;
import io.concert.common.api.TransitionResult;
import io.concert.sdk.ModelStateMachineTest.TicketMachine;
import io.concert.sink.EntitySnapshot;
import io.concert.store.InMemoryStateStore;
import io.temporal.activity.Activity;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.client.UpdateOptions;
import io.temporal.client.WithStartWorkflowOperation;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowUpdateStage;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The completed-entity snapshot publish of {@link ModelStateMachine} (ticket machine: CLOSED is terminal). */
class EntitySnapshotPublishTest {

    /** Captures snapshots; fails the first {@code failures} attempts. */
    static final class FakePublisher implements EntitySnapshotPublisher {
        final List<EntitySnapshot> published = new CopyOnWriteArrayList<>();
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
        volatile String runId;

        @Override
        public void publish(EntitySnapshot snapshot) {
            attempts.incrementAndGet();
            runId = Activity.getExecutionContext().getInfo().getWorkflowRunId();
            if (failures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IllegalStateException("kafka down");
            }
            published.add(snapshot);
        }
    }

    private TestWorkflowEnvironment env;
    private InMemoryStateStore store;
    private WorkflowClient client;
    private final FakePublisher publisher = new FakePublisher();

    @BeforeEach
    void setUp() {
        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "false");
        env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setUseTimeskipping(false).build());
        store = new InMemoryStateStore();
        Worker w = env.newWorker(TaskQueues.stateMachine("ticket"));
        w.registerWorkflowImplementationTypes(TicketMachine.class);
        w.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store), publisher);
        env.start();
        client = env.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        env.close();
    }

    private TransitionResult send(String id, String type, String payload) {
        EntityWorkflow wf = client.newWorkflowStub(EntityWorkflow.class, WorkflowOptions.newBuilder()
                .setWorkflowId(WorkflowIds.entity("ticket", "t1"))
                .setTaskQueue(TaskQueues.stateMachine("ticket"))
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                .build());
        EventEnvelope e = new EventEnvelope(id, "ticket", "t1", type, List.of(), payload, 0, 0);
        return WorkflowClient.executeUpdateWithStart(wf::handle, e,
                UpdateOptions.<TransitionResult>newBuilder().setUpdateId(id).setWaitForStage(WorkflowUpdateStage.COMPLETED).build(),
                new WithStartWorkflowOperation<>(wf::run, EntityInit.fresh("ticket", "t1")));
    }

    private static void await(String what, long timeoutMs, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            LockSupport.parkNanos(20_000_000);
        }
    }

    /** Assign, resolve, close: three transitions, which is also when TicketMachine continues as new. */
    private void closeTicket() {
        assertTrue(send("e1", "assign", "{\"assignee\":\"ann\"}").accepted());
        assertTrue(send("e2", "resolve", "{\"resolution\":\"fixed\"}").accepted());
        assertTrue(send("e3", "close", null).accepted());
    }

    @Test
    void terminalTransitionPublishesOneSnapshot() {
        assertTrue(send("e1", "assign", "{\"assignee\":\"ann\"}").accepted());
        List<TransitionRecord> recent = client.newWorkflowStub(EntityWorkflow.class, "ticket:t1").snapshot().recent();
        long firstTransition = recent.getFirst().tsMillis();
        assertTrue(send("e2", "resolve", "{\"resolution\":\"fixed\"}").accepted());
        assertTrue(send("e3", "close", null).accepted());
        await("snapshot", 10_000, () -> !publisher.published.isEmpty());

        EntitySnapshot s = publisher.published.getFirst();
        assertEquals("ticket:t1", s.entityId());
        assertEquals("ticket", s.smType());
        assertEquals("CLOSED", s.state());
        assertEquals(3, s.version());
        assertEquals(Instant.ofEpochMilli(firstTransition), s.createdAt());
        assertNotNull(s.completedAt());
        assertFalse(s.completedAt().isBefore(s.createdAt()));
        assertEquals(store.loadState("ticket:t1").orElseThrow().data(), s.modelJson());

        // Rejected events in the terminal state (handled by the continued-as-new run) publish nothing more.
        assertFalse(send("e4", "close", null).accepted());
        LockSupport.parkNanos(500_000_000);
        assertEquals(1, publisher.published.size());
        assertEquals(1, publisher.attempts.get());
    }

    @Test
    void nonTerminalTransitionsDoNotPublish() {
        assertTrue(send("e1", "assign", "{\"assignee\":\"ann\"}").accepted());
        assertTrue(send("e2", "resolve", "{\"resolution\":\"fixed\"}").accepted());
        assertTrue(send("e3", "reopen", null).accepted());
        LockSupport.parkNanos(500_000_000);
        assertEquals(0, publisher.attempts.get());
    }

    @Test
    void createdAtSurvivesContinueAsNew() {
        assertTrue(send("e1", "assign", "{\"assignee\":\"ann\"}").accepted());
        long firstTransition = client.newWorkflowStub(EntityWorkflow.class, "ticket:t1").snapshot().recent().getFirst().tsMillis();
        assertTrue(send("e2", "resolve", "{\"resolution\":\"fixed\"}").accepted());
        assertTrue(send("e3", "reopen", null).accepted()); // continue-as-new after this one
        assertTrue(send("e4", "resolve", "{\"resolution\":\"really fixed\"}").accepted());
        assertTrue(send("e5", "close", null).accepted());
        await("snapshot", 10_000, () -> !publisher.published.isEmpty());
        EntitySnapshot s = publisher.published.getFirst();
        assertEquals(5, s.version());
        assertEquals(Instant.ofEpochMilli(firstTransition), s.createdAt());
    }

    @Test
    void failingPublishIsRetriedWithoutBlockingUpdates() {
        publisher.failures.set(2); // retried after 1 s and 2 s
        closeTicket();
        // The terminal update completed although the publish has not succeeded yet, and later updates
        // are handled meanwhile.
        assertTrue(publisher.published.isEmpty());
        assertFalse(send("e4", "assign", "{\"assignee\":\"bob\"}").accepted());
        assertTrue(publisher.published.isEmpty());
        await("publish after retries", 15_000, () -> !publisher.published.isEmpty());
        assertEquals(3, publisher.attempts.get());
        assertEquals("CLOSED", publisher.published.getFirst().state());
    }

    @Test
    void continueAsNewWaitsForPendingPublish() {
        publisher.failures.set(2);
        closeTicket(); // third event: continue-as-new is due, but the publish is still retrying
        await("first attempt", 5_000, () -> publisher.runId != null);
        String firstRun = publisher.runId;
        await("continue-as-new", 15_000, () -> lastEvent(firstRun) == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW);

        List<EventType> types = client.fetchHistory("ticket:t1", firstRun).getEvents().stream()
                .map(HistoryEvent::getEventType).toList();
        int completed = types.indexOf(EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED);
        assertTrue(completed >= 0, types.toString());
        assertTrue(completed < types.indexOf(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW), types.toString());
        assertEquals(1, publisher.published.size());

        // the continued run carries on (and does not publish again)
        assertTrue(send("e4", "close", null).message().startsWith("no transition"));
    }

    private EventType lastEvent(String runId) {
        List<HistoryEvent> events = client.fetchHistory("ticket:t1", runId).getEvents();
        return events.isEmpty() ? null : events.getLast().getEventType();
    }
}
