package io.concert.samples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.TaskQueues;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EntityInit;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.TransitionResult;
import io.concert.model.runtime.ModelJson;
import io.concert.samples.model.demo.common.Currency;
import io.concert.samples.model.demo.common.PaymentMethod;
import io.concert.samples.model.demo.order.Order;
import io.concert.samples.model.demo.order.OrderStatus;
import io.concert.samples.model.demo.payment.EntryKind;
import io.concert.samples.model.demo.payment.Payment;
import io.concert.samples.model.demo.shipment.Shipment;
import io.concert.sdk.EntityPersistenceActivitiesImpl;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.sink.EntitySnapshot;
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
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TypedSampleMachinesTest {

    private TestWorkflowEnvironment env;
    private InMemoryStateStore store;
    private WorkflowClient client;
    /** Snapshots published on terminal transitions (the real worker sends them to Kafka). */
    private final List<EntitySnapshot> snapshots = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "false");
        env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setUseTimeskipping(false).build());
        store = new InMemoryStateStore();
        SampleMachines.ALL.forEach((type, m) -> {
            Worker w = env.newWorker(TaskQueues.stateMachine(type));
            w.registerWorkflowImplementationTypes(m.impl());
            w.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store), new SimulatedWork.Impl(),
                    (EntitySnapshotPublisher) snapshots::add);
        });
        env.start();
        client = env.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        env.close();
    }

    private TransitionResult send(String smType, String key, String eventId, String type, String payload) {
        EntityWorkflow wf = client.newWorkflowStub(EntityWorkflow.class, WorkflowOptions.newBuilder()
                .setWorkflowId(WorkflowIds.entity(smType, key))
                .setTaskQueue(TaskQueues.stateMachine(smType))
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                .build());
        EventEnvelope e = new EventEnvelope(eventId, smType, key, type, List.of(), payload, 0, 0);
        return WorkflowClient.executeUpdateWithStart(wf::handle, e,
                UpdateOptions.<TransitionResult>newBuilder().setUpdateId(eventId).setWaitForStage(WorkflowUpdateStage.COMPLETED).build(),
                new WithStartWorkflowOperation<>(wf::run, EntityInit.fresh(smType, key)));
    }

    private EntitySnapshot awaitSnapshot(String entityId) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (EntitySnapshot s : snapshots) {
                if (s.entityId().equals(entityId)) {
                    return s;
                }
            }
            LockSupport.parkNanos(20_000_000);
        }
        throw new AssertionError("no snapshot for " + entityId + " in " + snapshots);
    }

    private <T> T data(String entityKey, Class<T> type) {
        return ModelJson.read(store.loadState(entityKey).orElseThrow().data(), type);
    }

    @Test
    void nullAndWorkOnlyPayloadsGetDefaults() {
        // EndToEndIT sends null payloads, ScalingBench sends {"workMs":40}
        assertTrue(send("order", "o1", "e1", "pay", null).accepted());
        assertTrue(send("order", "o1", "e2", "ship", "{\"workMs\":5}").accepted());
        assertTrue(send("order", "o1", "e3", "deliver", "{\"workMs\":5}").accepted());
        Order order = data("order:o1", Order.class);
        assertEquals(OrderStatus.DELIVERED, order.getStatus());
        assertEquals(PaymentMethod.CARD, order.getPaymentMethod());
        assertEquals(0, order.getTotal().getAmount().signum());
        assertEquals("TRK-o1", order.getTrackingNumber());
        assertNotNull(order.getDeliveredAt());
        EntitySnapshot snap = awaitSnapshot("order:o1");
        assertEquals("DELIVERED", snap.state());
        assertEquals(3, snap.version());
        assertEquals(store.loadState("order:o1").orElseThrow().data(), snap.modelJson());

        assertTrue(send("payment", "p1", "p-1", "authorize", null).accepted());
        assertTrue(send("payment", "p1", "p-2", "capture", null).accepted());
        assertTrue(send("payment", "p1", "p-3", "refund", "{\"workMs\":5}").accepted());
        assertEquals(List.of(EntryKind.AUTHORIZATION, EntryKind.CAPTURE, EntryKind.REFUND),
                data("payment:p1", Payment.class).getEntries().stream().map(e -> e.getKind()).toList());

        for (String[] step : new String[][] {{"pick", "s-1"}, {"dispatch", "s-2"}, {"deliver", "s-3"}}) {
            assertTrue(send("shipment", "s1", step[1], step[0], "{\"workMs\":5}").accepted(), step[0]);
        }
        assertEquals("DELIVERED", store.loadState("shipment:s1").orElseThrow().state());
        assertEquals("UNASSIGNED", data("shipment:s1", Shipment.class).getCarrier());
    }

    @Test
    void checkoutSetsLinesAndTotalAndChecksTheAmount() {
        String lines = "\"lines\":[{\"sku\":\"A\",\"quantity\":2,\"unitPrice\":{\"amount\":\"10.50\",\"currency\":\"EUR\"}},"
                + "{\"sku\":\"B\",\"unitPrice\":{\"amount\":4,\"currency\":\"EUR\"}}]";
        TransitionResult wrong = send("order", "o2", "e1", "pay",
                "{\"amount\":{\"amount\":20,\"currency\":\"EUR\"},\"method\":\"WALLET\"," + lines + "}");
        assertFalse(wrong.accepted());
        assertEquals("payment of 20 EUR does not match order total 25.00 EUR", wrong.message());
        assertEquals(0, store.loadState("order:o2").orElseThrow().version());

        TransitionResult ok = send("order", "o2", "e2", "pay",
                "{\"amount\":{\"amount\":25,\"currency\":\"EUR\"},\"method\":\"WALLET\",\"customer\":{\"customerId\":\"c1\",\"name\":\"Ann\"},"
                        + lines + "}");
        assertTrue(ok.accepted(), ok.message());
        Order order = data("order:o2", Order.class);
        assertEquals(2, order.getLines().size());
        assertEquals(order, order.getLines().getFirst().getOrder());
        assertEquals(0, new BigDecimal("25").compareTo(order.getTotal().getAmount()));
        assertEquals(Currency.EUR, order.getTotal().getCurrency());
        assertEquals("Ann", order.getCustomer().getName());

        TransitionResult badShip = send("order", "o2", "e3", "ship", "{\"carrier\":\"DHL\",\"trackingNumber\":7,\"extra\":true}");
        assertTrue(badShip.accepted(), badShip.message()); // numbers coerce to String; unknown fields are ignored
        TransitionResult invalid = send("order", "o2", "e4", "deliver", "{\"signedBy\":[1]}");
        assertFalse(invalid.accepted());
        assertTrue(invalid.message().startsWith("invalid DeliverCommand payload"), invalid.message());
    }

    @Test
    void invalidLinesAreRejectedByPayloadValidation() {
        TransitionResult r = send("order", "o3", "e1", "pay", "{\"lines\":[{\"sku\":\"A\"}]}");
        assertFalse(r.accepted());
        assertEquals("invalid payload: PayCommand.lines[0].unitPrice: required [1]", r.message());
    }

    @Test
    void captureCannotExceedAuthorization() {
        send("payment", "p2", "e1", "authorize", "{\"amount\":{\"amount\":10},\"method\":\"CARD\"}");
        TransitionResult r = send("payment", "p2", "e2", "capture", "{\"amount\":{\"amount\":11}}");
        assertFalse(r.accepted());
        assertEquals("capture of 11 USD exceeds authorized 10 USD", r.message());
    }

    @Test
    void modelDiagramsAreRegistered() {
        String mermaid = SampleMachines.get("order").modelMermaid();
        assertTrue(mermaid.startsWith("classDiagram\n  class Customer"), mermaid);
        assertTrue(mermaid.contains("class Money"), mermaid);
        assertEquals(1, mermaid.split("classDiagram", -1).length - 1);
        assertEquals(java.util.Set.of("DELIVERED", "CANCELLED"), OrderStateMachine.SPEC.terminalStates());
    }
}
