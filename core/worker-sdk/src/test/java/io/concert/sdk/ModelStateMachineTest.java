package io.concert.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.TaskQueues;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EntityInit;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.SmStateRow;
import io.concert.common.api.TransitionResult;
import io.concert.model.runtime.LegendModel;
import io.concert.model.runtime.ModelJson;
import io.concert.sdk.model.demo.ticket.AssignCommand;
import io.concert.sdk.model.demo.ticket.Comment;
import io.concert.sdk.model.demo.ticket.CommentCommand;
import io.concert.sdk.model.demo.ticket.Priority;
import io.concert.sdk.model.demo.ticket.ResolveCommand;
import io.concert.sdk.model.demo.ticket.Ticket;
import io.concert.store.InMemoryStateStore;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.client.UpdateOptions;
import io.temporal.client.WithStartWorkflowOperation;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowUpdateHandle;
import io.temporal.client.WorkflowUpdateStage;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Public: the event-style interop tests reuse {@link TicketMachine}. */
public class ModelStateMachineTest {

    /** Event ids that reached a terminal state, recorded by {@link TicketMachine#onTerminal}. */
    static final Set<String> TERMINAL_EVENTS = ConcurrentHashMap.newKeySet();

    @LegendModel(file = "ticket.pure", root = "demo::ticket::Ticket")
    public static class TicketMachine extends ModelStateMachine<Ticket> {

        static final StateMachineSpec SPEC = StateMachineSpec.startingAt("OPEN")
                .on("OPEN", "assign", "ASSIGNED", AssignCommand.class)
                .on("ASSIGNED", "comment", "ASSIGNED", CommentCommand.class)
                .on("ASSIGNED", "resolve", "RESOLVED", ResolveCommand.class)
                .on("RESOLVED", "reopen", "ASSIGNED")
                .on("RESOLVED", "close", "CLOSED")
                .build();

        @Override
        protected StateMachineSpec spec() {
            return SPEC;
        }

        @Override
        protected int continueAsNewAfter() {
            return 3; // exercise continue-as-new in a short test
        }

        @Override
        protected Class<Ticket> dataType() {
            return Ticket.class;
        }

        @Override
        protected Ticket initialData(String instanceKey) {
            return new Ticket().setId(instanceKey);
        }

        @Override
        protected void onTransition(String from, String to, Ticket ticket, Object payload, EventEnvelope event) {
            switch (payload) {
                case AssignCommand a -> {
                    if (a.getAssignee().equals("nobody")) {
                        reject("cannot assign to nobody");
                    }
                    ticket.setAssignee(a.getAssignee());
                    if (a.getPriority() != null) {
                        ticket.setPriority(a.getPriority());
                    }
                }
                case CommentCommand c -> {
                    if (c.getText().startsWith("slow")) {
                        // A durable timer ends the workflow task, so the next update is delivered
                        // while this one waits; it must not be decided before this one commits.
                        Workflow.sleep(Duration.ofMillis(300));
                    }
                    ticket.addComment(new Comment().setAuthor(c.getAuthor()).setText(c.getText()));
                }
                case ResolveCommand r -> ticket.setResolution(r.getResolution())
                        .setResolvedAt(Instant.ofEpochMilli(Workflow.currentTimeMillis()));
                case null -> {
                    switch (event.eventType()) {
                        case "reopen" -> ticket.setResolution(null).setResolvedAt(null).setReopenCount(ticket.getReopenCount() + 1);
                        case "close" -> {}
                        default -> reject(event.eventType() + " requires a payload");
                    }
                }
                default -> throw new IllegalStateException("unexpected payload " + payload);
            }
        }

        @Override
        protected void onTerminal(String state, Ticket data, EventEnvelope event) {
            TERMINAL_EVENTS.add(event.eventId() + ":" + state + ":" + data.getResolution());
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
        Worker w = env.newWorker(TaskQueues.stateMachine("ticket"));
        w.registerWorkflowImplementationTypes(TicketMachine.class);
        w.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store), EntitySnapshotPublisher.logging());
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

    private WorkflowUpdateHandle<TransitionResult> startSend(String id, String type, String payload) {
        EntityWorkflow wf = client.newWorkflowStub(EntityWorkflow.class, WorkflowIds.entity("ticket", "t1"));
        EventEnvelope e = new EventEnvelope(id, "ticket", "t1", type, List.of(), payload, 0, 0);
        return WorkflowClient.startUpdate(wf::handle, e,
                UpdateOptions.<TransitionResult>newBuilder().setUpdateId(id).setWaitForStage(WorkflowUpdateStage.ACCEPTED).build());
    }

    private SmStateRow row() {
        return store.loadState("ticket:t1").orElseThrow();
    }

    private Ticket ticket() {
        return ModelJson.read(row().data(), Ticket.class);
    }

    @Test
    void typedTransitionMutatesDataAndPersistsJson() {
        TransitionResult r = send("e1", "assign", "{\"assignee\":\"ann\",\"priority\":\"HIGH\"}");
        assertTrue(r.accepted(), r.message());
        assertEquals("ASSIGNED", row().state());
        assertEquals(1, row().version());
        // Deterministic JSON: sorted properties, defaults applied by initialData's generated defaults.
        assertEquals("{\"assignee\":\"ann\",\"comments\":[],\"id\":\"t1\",\"priority\":\"HIGH\",\"reopenCount\":0}", row().data());
    }

    @Test
    void payloadIsBoundToTheDeclaredType() {
        send("e1", "assign", "{\"assignee\":\"ann\"}");
        send("e2", "comment", "{\"author\":\"bob\",\"text\":\"looking\"}");
        Ticket t = ticket();
        assertEquals(Priority.NORMAL, t.getPriority());
        assertEquals("bob", t.getComments().getFirst().getAuthor());
        // read() relinks the back-reference
        assertEquals(t, t.getComments().getFirst().getTicket());
    }

    @Test
    void invalidDataRejectsAndLeavesStateUnchanged() {
        send("e1", "assign", "{\"assignee\":\"ann\"}");
        send("e2", "comment", "{\"author\":\"a\",\"text\":\"1\"}");
        send("e3", "comment", "{\"author\":\"a\",\"text\":\"2\"}");
        String before = row().data();
        TransitionResult r = send("e4", "comment", "{\"author\":\"a\",\"text\":\"3\"}");
        assertFalse(r.accepted());
        assertEquals("invalid data: Ticket.comments: expected [0..2] values but found 3", r.message());
        assertEquals("ASSIGNED", r.toState());
        assertEquals(3, r.version());
        assertEquals(3, row().version());
        assertEquals(before, row().data());
        // the next valid event starts from the unchanged data
        TransitionResult resolved = send("e5", "resolve", "{\"resolution\":\"fixed\"}");
        assertTrue(resolved.accepted(), resolved.message());
        assertEquals(4, resolved.version());
        assertEquals(2, ticket().getComments().size());
    }

    @Test
    void invalidPayloadIsRejected() {
        TransitionResult badEnum = send("e1", "assign", "{\"assignee\":\"ann\",\"priority\":\"URGENT\"}");
        assertFalse(badEnum.accepted());
        assertTrue(badEnum.message().startsWith("invalid AssignCommand payload: "), badEnum.message());
        TransitionResult badJson = send("e2", "assign", "{not json");
        assertFalse(badJson.accepted());
        assertTrue(badJson.message().startsWith("invalid AssignCommand payload: "), badJson.message());
        TransitionResult missing = send("e3", "assign", "{}");
        assertFalse(missing.accepted());
        assertEquals("invalid payload: AssignCommand.assignee: required [1]", missing.message());
        TransitionResult nullPayload = send("e4", "assign", null);
        assertFalse(nullPayload.accepted());
        assertEquals("assign requires a payload", nullPayload.message());
        TransitionResult rejected = send("e5", "assign", "{\"assignee\":\"nobody\"}");
        assertFalse(rejected.accepted());
        assertEquals("cannot assign to nobody", rejected.message());
        assertEquals("OPEN", row().state());
        assertEquals(0, row().version());
        assertNull(row().data());
        assertTrue(send("e6", "assign", "{\"assignee\":\"ann\"}").accepted());
    }

    @Test
    void continueAsNewKeepsTypedData() {
        send("e1", "assign", "{\"assignee\":\"ann\"}");
        send("e2", "comment", "{\"author\":\"a\",\"text\":\"1\"}");
        send("e3", "resolve", "{\"resolution\":\"fixed\"}");
        // continue-as-new happened after 3 events; the next run starts from the carried-over JSON
        TransitionResult r = send("e4", "reopen", null);
        assertTrue(r.accepted(), r.message());
        assertEquals(4, r.version());
        Ticket t = ticket();
        assertEquals("ann", t.getAssignee());
        assertEquals(1, t.getComments().size());
        assertEquals(1L, t.getReopenCount());
        assertNull(t.getResolution());
    }

    @Test
    void yieldingTransitionKeepsAcceptanceOrder() {
        send("e1", "assign", "{\"assignee\":\"ann\"}");
        WorkflowUpdateHandle<TransitionResult> first = startSend("e2", "comment", "{\"author\":\"a\",\"text\":\"slow\"}");
        WorkflowUpdateHandle<TransitionResult> second = startSend("e3", "comment", "{\"author\":\"a\",\"text\":\"fast\"}");
        assertEquals(2, first.getResult().version());
        assertEquals(3, second.getResult().version());
        assertEquals(List.of("slow", "fast"), ticket().getComments().stream().map(Comment::getText).toList());
    }

    @Test
    void terminalStatesAreDetected() {
        assertEquals(Set.of("CLOSED"), TicketMachine.SPEC.terminalStates());
        assertTrue(TicketMachine.SPEC.isTerminal("CLOSED"));
        assertFalse(TicketMachine.SPEC.isTerminal("ASSIGNED"));
        assertEquals(ResolveCommand.class, TicketMachine.SPEC.payloadType("ASSIGNED", "resolve").orElseThrow());
        assertTrue(TicketMachine.SPEC.payloadType("RESOLVED", "close").isEmpty());

        send("t-1", "assign", "{\"assignee\":\"ann\"}");
        send("t-2", "resolve", "{\"resolution\":\"fixed\"}");
        assertTrue(TERMINAL_EVENTS.stream().noneMatch(e -> e.startsWith("t-")));
        assertTrue(send("t-3", "close", null).accepted());
        assertTrue(TERMINAL_EVENTS.contains("t-3:CLOSED:fixed"), TERMINAL_EVENTS.toString());
        assertTrue(row().data().contains("\"resolvedAt\":\""), row().data());
    }
}
