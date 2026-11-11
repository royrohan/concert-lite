package io.concert.eco.payments;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.common.TaskQueues;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EntityInit;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.LockRequest;
import io.concert.common.api.TransitionResult;
import io.concert.eco.payments.external.InMemoryExternals;
import io.concert.eco.payments.external.NotifyActivities;
import io.concert.sdk.EntityPersistenceActivitiesImpl;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.sdk.events.EnqueueRequest;
import io.concert.sdk.events.EventEnqueueActivities;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The settlement machine against in-memory stand-ins of the ledger and Kafka, on Temporal's test server: what Temporal's
 * retries, durable history and the Saga do when the external services commit but the answer is lost.
 */
class SettlementMachineTest {

    private static final String SETTLE = "{\"accountId\":\"ACC-1\",\"amount\":125.50,\"currency\":\"EUR\",\"memo\":\"invoice 7\"}";

    private TestWorkflowEnvironment env;
    private WorkflowClient client;
    private final InMemoryStateStore store = new InMemoryStateStore();
    private final InMemoryExternals.Ledger ledger = new InMemoryExternals.Ledger();
    private final InMemoryExternals.Topic topic = new InMemoryExternals.Topic();
    /** Events the machine emitted (its outcome events), in order. */
    private final List<EventEnvelope> emitted = new CopyOnWriteArrayList<>();
    private final AtomicInteger ids = new AtomicInteger();

    private void start(boolean timeSkipping) {
        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "false");
        env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setUseTimeskipping(timeSkipping).build());
        Worker worker = env.newWorker(TaskQueues.stateMachine("settlement"));
        worker.registerWorkflowImplementationTypes(SettlementMachine.class);
        worker.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store), (EntitySnapshotPublisher) s -> {},
                ledger, topic, new EventEnqueueActivities() {
                    @Override
                    public void enqueue(EnqueueRequest request) {
                        request.locks().stream().map(LockRequest::event).filter(e -> !emitted.contains(e)).forEach(emitted::add);
                    }

                    @Override
                    public void unblock(String lockKey, String requestId) {}
                });
        env.start();
        client = env.getWorkflowClient();
    }

    @AfterEach
    void stop() {
        env.close();
    }

    @Test
    void settlesWithOneChargeAndOneRecord() {
        start(false);
        assertTrue(send(event("P-1", "settle", SETTLE)).accepted());
        assertEquals("SETTLED", deliverOutcome("P-1"));

        assertEquals(1, ledger.committed.size());
        assertEquals(1, topic.records.size());
        JsonNode p = data("P-1");
        assertEquals("LDG-P-1", p.path("ledgerRef").asText());
        assertEquals(1, p.path("ledgerAttempts").asInt());
        assertEquals(1, p.path("kafkaAttempts").asInt());
    }

    @Test
    void aChargeCommittedWhoseAnswerWasLostIsRetriedAndCommittedOnce() {
        start(false);
        ledger.loseAnswers.set(2); // the first two attempts commit (well, the first does) and then fail

        send(event("P-2", "settle", SETTLE));

        assertEquals("SETTLED", deliverOutcome("P-2"));
        assertEquals(3, ledger.requests.get());
        assertEquals(1, ledger.committed.size(), "one commit for three requests with the same idempotency key");
        JsonNode p = data("P-2");
        assertEquals(3, p.path("ledgerAttempts").asInt());
        assertTrue(p.path("ledgerReplayed").asBoolean(), "the third attempt got the stored answer");
    }

    @Test
    void aRecordWrittenWhoseAckWasLostIsWrittenAgainWithTheSameIdempotencyKey() {
        start(false);
        topic.loseAcks.set(1);

        send(event("P-3", "settle", SETTLE));

        assertEquals("SETTLED", deliverOutcome("P-3"));
        assertEquals(2, topic.records.size(), "at least once: the retry writes the record again");
        assertEquals(1, topic.records.stream().map(NotifyActivities.PublishRequest::idempotencyKey).distinct().count(),
                "consumers deduplicate on the key: effectively once");
        assertEquals(2, data("P-3").path("kafkaAttempts").asInt());
        assertEquals(1, ledger.committed.size(), "the charge before it was not repeated");
    }

    @Test
    void aDeclinedChargeIsNotRetriedAndCommitsNothing() {
        start(false);
        send(event("P-4", "settle", "{\"accountId\":\"ACC-1\",\"amount\":25000,\"currency\":\"EUR\"}"));

        assertEquals("DECLINED", deliverOutcome("P-4"));
        assertEquals(1, ledger.requests.get());
        assertTrue(ledger.committed.isEmpty());
        assertTrue(topic.records.isEmpty());
        assertTrue(data("P-4").path("reason").asText().contains("insufficient funds"));
    }

    @Test
    void aSettlementThatCannotBePublishedIsUndoneBySaga() {
        start(true); // skips the 40 s of publish retries
        topic.refuseSettled = true;

        send(event("P-5", "settle", SETTLE));

        assertEquals("FAILED", deliverOutcome("P-5"));
        assertEquals(List.of("P-5"), ledger.refunded, "the charge was refunded");
        assertEquals(List.of("REVERSED"), topic.records.stream().map(NotifyActivities.PublishRequest::type).toList(),
                "a reversal is published, in case a settlement record was written");
        JsonNode p = data("P-5");
        assertTrue(p.path("refunded").asBoolean());
        assertTrue(p.path("reason").asText().contains("broker unavailable"), p.path("reason").asText());
    }

    @Test
    void aRetriedSettleEventIsAppliedOnce() {
        start(false);
        EventEnvelope settle = event("P-6", "settle", SETTLE);
        send(settle);
        TransitionResult again = send(settle); // the coordinator re-dispatching the same event

        assertTrue(again.accepted());
        assertEquals(1, ledger.requests.get(), "the update id dedupes the event: the activities ran once");
        assertEquals(1, emitted.size());
    }

    // ------------------------------------------------------------------------------------------------------------

    private EventEnvelope event(String id, String type, String payload) {
        EventEnvelope e = PaymentsEvents.envelope("settlement", type, id, payload);
        return new EventEnvelope("test-" + ids.incrementAndGet(), e.smType(), e.instanceKey(), e.eventType(), e.lockKeys(),
                e.payload(), 0, 0);
    }

    /** Waits for the outcome event the settle transition emitted, delivers it like the coordinator would, returns the state. */
    private String deliverOutcome(String id) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (emitted.stream().noneMatch(e -> e.instanceKey().equals(id)) && System.currentTimeMillis() < deadline) {
            Thread.onSpinWait();
        }
        EventEnvelope outcome = emitted.stream().filter(e -> e.instanceKey().equals(id)).findFirst().orElseThrow();
        TransitionResult r = send(outcome);
        assertTrue(r.accepted(), r.toString());
        assertFalse(r.toState().isEmpty());
        return r.toState();
    }

    private TransitionResult send(EventEnvelope event) {
        EntityWorkflow wf = client.newWorkflowStub(EntityWorkflow.class, WorkflowOptions.newBuilder()
                .setWorkflowId(WorkflowIds.entity("settlement", event.instanceKey()))
                .setTaskQueue(TaskQueues.stateMachine("settlement"))
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                .build());
        return WorkflowClient.executeUpdateWithStart(wf::handle, event,
                UpdateOptions.<TransitionResult>newBuilder().setUpdateId(event.eventId()).setWaitForStage(WorkflowUpdateStage.COMPLETED).build(),
                new WithStartWorkflowOperation<>(wf::run, EntityInit.fresh("settlement", event.instanceKey())));
    }

    private JsonNode data(String id) {
        try {
            return Json.MAPPER.readTree(store.loadState(WorkflowIds.entity("settlement", id)).orElseThrow().data());
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
