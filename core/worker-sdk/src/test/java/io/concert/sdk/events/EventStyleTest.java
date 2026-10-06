package io.concert.sdk.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import io.concert.common.EventRouting;
import io.concert.common.Json;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EventLifecycle;
import io.concert.common.api.EventOutcome;
import io.concert.common.api.KeyLockWorkflow;
import io.concert.common.api.ProcessRequest;
import io.concert.common.api.SmStateRow;
import io.concert.orchestration.KeyLockWorkflowImpl;
import io.concert.orchestration.events.EventOps;
import io.concert.sink.EntitySnapshot;
import io.temporal.client.WorkflowStub;
import io.temporal.testing.WorkflowReplayer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Event-style processing end to end: ingest -> lock chain -> processor -> handlers -> StateStore. */
class EventStyleTest {

    private EventFixture f;

    @BeforeEach
    void setUp() {
        f = new EventFixture(false, 500);
    }

    @AfterEach
    void tearDown() {
        f.close();
    }

    private static JsonNode data(SmStateRow row) {
        return Json.read(row.data(), JsonNode.class);
    }

    private static List<String> texts(JsonNode array) {
        List<String> out = new ArrayList<>();
        array.forEach(n -> out.add(n.asText()));
        return out;
    }

    @Test
    void happyPathAcrossDomainsWritesLifecycleRowsStateAndSnapshots() {
        f.send(Shop.event("r-1", Shop.STOCK, new Shop.Restock("sku1", 5), "sku:sku1"));
        f.awaitStatus("r-1", "DONE");
        f.send(Shop.event("o-1", Shop.SHOP, new Shop.OrderPlaced("o1", "sku1", 2), "order:o1"));
        f.awaitStatus("o-1.1.1", "DONE");

        SmStateRow placed = f.row("o-1").orElseThrow();
        assertEquals("evt_order_placed", placed.smType());
        assertEquals("DONE", placed.state());
        JsonNode d = data(placed);
        assertEquals("ok", d.get("outcome").asText());
        assertEquals(List.of("o-1.1"), texts(d.get("children")));
        assertEquals(List.of("order:o1"), texts(d.get("keys")));
        assertEquals("evproc:shop:order:o1", d.get("processor").asText());
        assertEquals("o1", d.get("payload").get("orderId").asText());

        JsonNode reserve = data(f.row("o-1.1").orElseThrow());
        assertEquals("stock", reserve.get("domain").asText());
        assertEquals("o-1", reserve.get("parentEventId").asText());
        assertEquals("o-1", reserve.get("causationRoot").asText());
        assertEquals(List.of("sku:sku1"), texts(reserve.get("keys")));
        JsonNode confirmed = data(f.row("o-1.1.1").orElseThrow());
        assertEquals("o-1.1", confirmed.get("parentEventId").asText());
        assertEquals("o-1", confirmed.get("causationRoot").asText());

        SmStateRow order = f.store.loadState("state:order:o1").orElseThrow();
        assertEquals("st_order_state", order.smType());
        assertEquals("o-1.1.1", order.lastEventId());
        assertEquals(2, order.version());
        Shop.OrderState os = Json.read(order.data(), Shop.OrderState.class);
        assertEquals("CONFIRMED", os.status);
        assertEquals(2, os.events);
        assertEquals(3, f.state("sku:sku1", Shop.StockState.class).available);

        EventFixture.await("snapshots", Duration.ofSeconds(10), () -> f.publisher.of("event:o-1.1.1").size() == 1
                && f.publisher.of("state:order:o1").size() == 2);
        EntitySnapshot s = f.publisher.of("event:o-1").getFirst();
        assertEquals("evt_order_placed", s.smType());
        assertEquals("DONE", s.state());
        assertEquals(placed.version(), s.version());
        // traces: the lock chain and the processor both report
        EventFixture.await("trace", Duration.ofSeconds(5), () -> f.store.traceForEvent("o-1.1").stream()
                .map(t -> t.stage().name()).toList().containsAll(List.of("EMITTED", "LOCK_GRANTED", "DONE")));
    }

    @Test
    void chainHasDeterministicIdsAndReplaysDeterministically() throws Exception {
        f.send(Shop.event("s1", Shop.SHOP, new Shop.Step("c1", 1), "chain:c1"));
        f.awaitStatus("s1.1.1", "DONE");
        assertEquals("DONE", f.status("s1"));
        assertEquals("DONE", f.status("s1.1"));
        Shop.Counter c = f.counter("chain:c1");
        assertEquals(3, c.value);
        assertEquals(List.of("s1", "s1.1", "s1.1.1"), c.seen);
        assertEquals(0, c.outOfOrder);
        EventFixture.sleep(300);
        assertEquals(1, Shop.RUNS.get("s1").get());
        assertEquals(1, Shop.RUNS.get("s1.1.1").get());
        assertNull(f.row("s1.1.1.1").orElse(null));

        f.replayProcessor(f.client.fetchHistory(WorkflowIds.processor(Shop.SHOP, "chain:c1")));
        WorkflowReplayer.replayWorkflowExecution(f.client.fetchHistory(WorkflowIds.lock("chain:c1")), KeyLockWorkflowImpl.class);
    }

    @Test
    void localActivityRetriesApplyWritesAndChildrenOnce() {
        Shop.FAILS.put("f1", 2);
        f.send(Shop.event("f-1", Shop.SHOP, new Shop.Flaky("f1"), "counter:f1"));
        f.awaitStatus("f-1.1", "DONE");
        assertEquals(3, data(f.row("f-1").orElseThrow()).get("attempts").asInt());
        assertEquals(3, Shop.RUNS.get("f-1").get());
        assertEquals(1, f.counterValue("counter:f1"));
        assertEquals(List.of("f-1"), f.counter("counter:f1").seen);
        assertEquals(1, f.counterValue("counter:f1-child"));
    }

    @Test
    void redeliveryOfADoneEventAppliesNothing() throws Exception {
        f.send(Shop.event("f-2", Shop.SHOP, new Shop.Flaky("f2"), "counter:f2"));
        f.awaitStatus("f-2.1", "DONE");
        EventEnvelope e = Shop.event("f-2", Shop.SHOP, new Shop.Flaky("f2"), "counter:f2");

        // same processor run: remembered as done
        EventOutcome again = EventRouting.process(f.client, new ProcessRequest("f-2#dup", e, "counter:f2", Map.of()));
        assertEquals(EventLifecycle.DONE, again.status());
        assertEquals("already done", again.detail());

        // a fresh processor run (e.g. after a crash, or an idle completion): the DONE row stops it, children are
        // re-offered and dropped by their own DONE rows
        WorkflowStub.fromTyped(EventRouting.processor(f.client, Shop.SHOP, "counter:f2")).terminate("simulate a lost run");
        EventOutcome fresh = EventRouting.process(f.client, new ProcessRequest("f-2#dup2", e, "counter:f2", Map.of()));
        assertEquals(EventLifecycle.DONE, fresh.status());
        assertEquals(List.of("f-2.1"), fresh.children());
        EventFixture.sleep(1000);
        assertEquals(1, Shop.RUNS.get("f-2").get());
        assertEquals(1, Shop.RUNS.get("f-2.1").get());
        assertEquals(1, f.counterValue("counter:f2"));
        assertEquals(1, f.counterValue("counter:f2-child"));
    }

    @Test
    void blockingErrorHoldsEveryKeyOfTheChainUntilRetried() throws Exception {
        Shop.POISONED.add("p1");
        f.send(Shop.event("p-1", Shop.SHOP, new Shop.Poison("p1"), "k:a", "k:b"));
        f.awaitStatus("p-1", "ERROR_BLOCKING");
        assertTrue(data(f.row("p-1").orElseThrow()).get("error").asText().contains("poisoned p1"));
        assertEquals(1, Shop.RUNS.get("p-1").get(), "BlockingError is not retried");

        f.send(Shop.event("c-a", Shop.SHOP, new Shop.Count("x", 1, -1), "k:a"),
                Shop.event("c-b", Shop.SHOP, new Shop.Count("x", 1, -1), "k:b"));
        EventFixture.sleep(1000);
        assertNull(f.status("c-a"), "k:a is held by the blocked chain");
        assertNull(f.status("c-b"), "k:b is held by the blocked head");
        assertTrue(f.client.newWorkflowStub(KeyLockWorkflow.class, WorkflowIds.lock("k:b")).snapshot().holderState()
                .startsWith("blocked"));

        // a retry that still fails stays blocked
        EventOps ops = new EventOps(f.client, f.store);
        assertEquals(EventLifecycle.ERROR_BLOCKING, ops.retry("p-1").status());
        assertEquals(1, ops.list(EventLifecycle.ERROR_BLOCKING).size());
        Shop.POISONED.remove("p1");
        EventOutcome ok = ops.retry("p-1");
        assertEquals(EventLifecycle.DONE, ok.status());
        f.awaitStatus("c-a", "DONE");
        f.awaitStatus("c-b", "DONE");
        assertEquals(List.of("p-1", "c-a"), f.counter("k:a").seen);
        assertEquals(List.of("c-b"), f.counter("k:b").seen);
        JsonNode d = data(f.row("p-1").orElseThrow());
        assertEquals(3, d.get("attempts").asInt());
        assertEquals(2, d.get("retries").asInt());
        assertEquals(0, ops.list(EventLifecycle.ERROR_BLOCKING).size());
        // the blocked head / unblock path of the lock and the retry path of the processor replay cleanly
        WorkflowReplayer.replayWorkflowExecution(f.client.fetchHistory(WorkflowIds.lock("k:b")), KeyLockWorkflowImpl.class);
        WorkflowReplayer.replayWorkflowExecution(f.client.fetchHistory(WorkflowIds.lock("k:a")), KeyLockWorkflowImpl.class);
        f.replayProcessor(f.client.fetchHistory(WorkflowIds.processor(Shop.SHOP, "k:a")));
    }

    @Test
    void skippingABlockedEventFreesItsKeys() {
        Shop.POISONED.add("p2");
        f.send(Shop.event("p-2", Shop.SHOP, new Shop.Poison("p2"), "k:s"));
        f.awaitStatus("p-2", "ERROR_BLOCKING");
        f.send(Shop.event("c-s", Shop.SHOP, new Shop.Count("x", 1, -1), "k:s"));
        EventFixture.sleep(500);
        assertNull(f.status("c-s"));

        EventOutcome skipped = new EventOps(f.client, f.store).skip("p-2", "bad data");
        assertEquals(EventLifecycle.DONE, skipped.status());
        f.awaitStatus("c-s", "DONE");
        JsonNode d = data(f.row("p-2").orElseThrow());
        assertEquals("skipped", d.get("outcome").asText());
        assertEquals("bad data", d.get("error").asText());
        assertEquals(List.of("c-s"), f.counter("k:s").seen, "the skipped event never applied");
    }

    @Test
    void nonBlockingErrorParksReleasesKeysAndRetriesThroughTheLockChain() {
        f.send(Shop.event("o-2", Shop.SHOP, new Shop.OrderPlaced("o2", "sku2", 1), "order:o2"));
        f.awaitStatus("o-2.1", "ERROR_NON_BLOCKING");
        assertEquals("PLACED", f.state("order:o2", Shop.OrderState.class).status);

        // its key is free: a restock on the same sku goes through
        f.send(Shop.event("r-2", Shop.STOCK, new Shop.Restock("sku2", 1), "sku:sku2"));
        f.awaitStatus("r-2", "DONE");

        EventOps ops = new EventOps(f.client, f.store);
        assertEquals(List.of("o-2.1"), ops.list(EventLifecycle.ERROR_NON_BLOCKING).stream().map(EventOps.EventRow::eventId).toList());
        EventOutcome retry = ops.retry("o-2.1");
        assertEquals(EventLifecycle.ERROR_NON_BLOCKING, retry.status());
        assertEquals("retrying as o-2.1~r1", retry.detail());
        f.awaitStatus("o-2.1", "DONE");
        f.awaitStatus("o-2.1.1", "DONE");
        assertEquals("CONFIRMED", f.state("order:o2", Shop.OrderState.class).status);
        assertEquals(0, f.state("sku:sku2", Shop.StockState.class).available);
        JsonNode d = data(f.row("o-2.1").orElseThrow());
        assertEquals(1, d.get("retries").asInt());
        assertEquals("o-2.1~r1", d.get("requestId").asText());
        EventFixture.await("LOCK_GRANTED of the retry", Duration.ofSeconds(5), () -> f.store.traceForEvent("o-2.1").stream()
                .filter(t -> t.stage().name().equals("LOCK_GRANTED")).count() == 2);
    }

    @Test
    void anEventWithoutHandlerIsParked() {
        f.send(EventEnvelope.event("n-1", Shop.SHOP, "Nope", List.of("nope:1"), "{}", System.currentTimeMillis()));
        f.awaitStatus("n-1", "ERROR_NON_BLOCKING");
        assertTrue(data(f.row("n-1").orElseThrow()).get("error").asText().startsWith("no handler for Nope"));
        f.send(Shop.event("n-2", Shop.SHOP, new Shop.Count("x", 1, -1), "nope:1"));
        f.awaitStatus("n-2", "DONE");
    }

    @Test
    void exhaustedRetriesBlockByDefaultAndParkWhenTheTypeSaysSo() {
        Shop.FAILS.put("fx", 99);
        Shop.FAILS.put("fy", 99);
        f.send(Shop.event("fx-1", Shop.SHOP, new Shop.Flaky("fx"), "fx:1"),
                Shop.event("fy-1", Shop.SHOP, new Shop.FlakyNb("fy"), "fy:1"));
        f.awaitStatus("fx-1", "ERROR_BLOCKING");
        f.awaitStatus("fy-1", "ERROR_NON_BLOCKING");
        JsonNode d = data(f.row("fx-1").orElseThrow());
        assertEquals(3, d.get("attempts").asInt());
        assertTrue(d.get("error").asText().contains("retries exhausted (3)"), d.toString());
        assertEquals(3, Shop.RUNS.get("fy-1").get());
        assertEquals(0, f.counterValue("fx:1"));
    }

    @Test
    void ordersEventsOnTheFirstKeyAcrossSingleAndMultiKeyEvents() {
        Random rnd = new Random(3);
        int n = 120;
        List<EventEnvelope> batch = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            List<String> keys = new ArrayList<>(List.of("q:1"));
            int extra = rnd.nextInt(3);
            for (int k = 0; k < extra; k++) {
                keys.add("q:" + (2 + rnd.nextInt(3)));
            }
            batch.add(Shop.event("q-" + i, Shop.SHOP, new Shop.Count("q", 1, i), keys.toArray(String[]::new)));
            if (batch.size() == 30) {
                f.dispatcher.dispatchBatch(batch);
                batch = new ArrayList<>();
            }
        }
        EventFixture.await("all applied", Duration.ofSeconds(90), () -> f.counterValue("q:1") == n);
        assertEquals(0, f.counter("q:1").outOfOrder);
        assertEquals(n - 1, f.counter("q:1").lastSeq);
        assertEquals(0, io.concert.orchestration.OverlapProbe.violations());
    }

    @Test
    void handlerDrivesAnEntityStateMachine() {
        f.send(Shop.event("t-1", Shop.SHOP, new Shop.OpenTicket("t9", "ann"), "ticket:t9"));
        EventFixture.await("ticket assigned", Duration.ofSeconds(30),
                () -> f.store.loadState("ticket:t9").map(r -> r.state().equals("ASSIGNED")).orElse(false));
        assertEquals("t-1.1", f.store.loadState("ticket:t9").orElseThrow().lastEventId());
        assertEquals(List.of("t-1.1"), texts(data(f.row("t-1").orElseThrow()).get("children")));
    }

    @Test
    void entityStateMachineEmitsAnEventStyleEvent() {
        f.send(new EventEnvelope("et-1", "eticket", "e1", "assign", List.of(), "{\"assignee\":\"bob\"}",
                System.currentTimeMillis(), System.currentTimeMillis()));
        f.awaitStatus("et-1.1", "DONE");
        assertEquals(1, f.counterValue("counter:eticket-e1"));
        JsonNode d = data(f.row("et-1.1").orElseThrow());
        assertEquals("et-1", d.get("parentEventId").asText());
        assertEquals("Count", d.get("eventType").asText());
        // a rejected transition emits nothing
        f.send(new EventEnvelope("et-2", "eticket", "e1", "assign", List.of(), "{\"assignee\":\"carl\"}",
                System.currentTimeMillis(), System.currentTimeMillis()));
        EventFixture.sleep(800);
        assertNull(f.status("et-2.1"));
        assertNotNull(f.store.loadState("eticket:e1").orElseThrow());
    }
}
