package io.concert.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.TraceRow;
import io.concert.common.TraceRow.Stage;
import io.concert.common.api.SmStateRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behavior every {@link StateStore} must provide. A backend test extends this and returns a store
 * (usually one shared instance against a container). Tests use a random prefix, so they can share a
 * database.
 */
public abstract class StateStoreContract {

    protected abstract StateStore store();

    /** Stores with native expiry (DynamoDB TTL) do not sweep synchronously. */
    protected boolean sweepsSynchronously() {
        return true;
    }

    private String p;

    @BeforeEach
    void prefix() {
        p = "t" + UUID.randomUUID().toString().substring(0, 8) + "-";
    }

    // ---------------------------------------------------------------- dedupe

    @Test
    void claimsNewIdsAndDropsDispatchedOnes() {
        StateStore s = store();
        assertEquals(Set.of(p + "a", p + "b"), s.claimForDispatch(List.of(p + "a", p + "b"), now()));
        s.markDispatched(List.of(p + "a"));
        // a was dispatched: duplicate. b is still RECEIVED (crash before dispatch): re-claim it.
        assertEquals(Set.of(p + "b", p + "c"), s.claimForDispatch(List.of(p + "a", p + "b", p + "c"), now()));
        assertEquals("DISPATCHED", s.processedStatus(p + "a").orElseThrow());
        assertEquals("RECEIVED", s.processedStatus(p + "c").orElseThrow());
        assertTrue(s.processedStatus(p + "never").isEmpty());
    }

    @Test
    void duplicateIdsInOneBatchAreClaimedOnce() {
        assertEquals(Set.of(p + "x"), store().claimForDispatch(List.of(p + "x", p + "x", p + "x"), now()));
        assertEquals(1L, store().processedStatusCounts(p).values().stream().mapToLong(Long::longValue).sum());
    }

    @Test
    void largeBatchesWork() {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            ids.add(p + i);
        }
        assertEquals(500, store().claimForDispatch(ids, now()).size());
        store().markDispatched(ids);
        assertEquals(0, store().claimForDispatch(ids, now()).size());
        assertEquals(500L, store().processedStatusCounts(p).get("DISPATCHED"));
    }

    @Test
    void sweepRemovesOnlyOldDedupeRows() {
        if (!sweepsSynchronously()) {
            return;
        }
        long old = now() - 48 * 3_600_000L;
        store().claimForDispatch(List.of(p + "old"), old);
        store().claimForDispatch(List.of(p + "new"), now());
        assertTrue(store().sweepProcessed(now() - 24 * 3_600_000L, 100) >= 1);
        assertTrue(store().processedStatus(p + "old").isEmpty());
        assertTrue(store().processedStatus(p + "new").isPresent());
    }

    // ---------------------------------------------------------------- checkpoints

    @Test
    void checkpointsRoundTripAndOverwrite() {
        assertTrue(store().loadCheckpoint(p + "stream", "shard-1").isEmpty());
        store().saveCheckpoint(p + "stream", "shard-1", "100");
        store().saveCheckpoint(p + "stream", "shard-1", "200");
        store().saveCheckpoint(p + "stream", "shard-2", "5");
        assertEquals("200", store().loadCheckpoint(p + "stream", "shard-1").orElseThrow());
        assertEquals("5", store().loadCheckpoint(p + "stream", "shard-2").orElseThrow());
    }

    // ---------------------------------------------------------------- entity state

    @Test
    void stateUpsertIsMonotonicByVersion() {
        String wf = "order:" + p + "o1";
        store().upsertState(row(wf, "PAID", 2), null);
        store().upsertState(row(wf, "CREATED", 1), null); // late retry of an older version: ignored
        assertEquals("PAID", store().loadState(wf).orElseThrow().state());
        store().upsertState(row(wf, "SHIPPED", 3), null);
        SmStateRow r = store().loadState(wf).orElseThrow();
        assertEquals("SHIPPED", r.state());
        assertEquals(3, r.version());
        assertEquals("e3", r.lastEventId());
        assertJsonEquals("{\"v\":3}", r.data());
    }

    @Test
    void concurrentOutOfOrderUpsertsConvergeOnHighestVersion() throws Exception {
        String wf = "order:" + p + "race";
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> fs = new ArrayList<>();
            for (int v = 50; v >= 1; v--) {
                int version = v;
                fs.add(pool.submit(() -> store().upsertState(row(wf, "S" + version, version), null)));
            }
            for (Future<?> f : fs) {
                f.get();
            }
        }
        assertEquals(50, store().loadState(wf).orElseThrow().version());
    }

    @Test
    void upsertWithTraceWritesBoth() {
        String wf = "order:" + p + "t";
        store().upsertState(row(wf, "PAID", 1), new TraceRow(p + "ev", now(), Stage.TRANSITION, wf, null, "CREATED -> PAID"));
        assertEquals("PAID", store().loadState(wf).orElseThrow().state());
        assertEquals(1, store().traceForEvent(p + "ev").size());
    }

    @Test
    void scansStatesByPrefix() {
        store().upsertState(row("ledger:" + p + "K1", "OPEN", 1), null);
        store().upsertState(row("ledger:" + p + "K2", "OPEN", 1), null);
        store().upsertState(row("ledger:other-" + p, "OPEN", 1), null);
        assertEquals(2, store().scanStates("ledger:" + p).size());
    }

    // ---------------------------------------------------------------- JSON data

    private static final String ORDER_JSON = """
            {"orderId":"o1","status":"PAID","total":{"amount":"120.50","currency":"EUR"},
             "customer":{"name":"Ada","vip":true,"loyaltyPoints":42},
             "lines":[{"sku":"A-1","qty":2,"price":10.25},{"sku":"B-2","qty":1,"price":99.99}],
             "notes":null,"tags":["gift","express"]}""";

    @Test
    void nestedJsonDataRoundTripsAsEquivalentJson() {
        String wf = "order:" + p + "json";
        store().upsertState(new SmStateRow(wf, "order", "PAID", ORDER_JSON, 1, "e1", now()), null);
        assertJsonEquals(ORDER_JSON, store().loadState(wf).orElseThrow().data());
    }

    @Test
    void nullAndNonJsonDataAreAccepted() {
        String a = "order:" + p + "null";
        store().upsertState(new SmStateRow(a, "order", "NEW", null, 1, "e1", now()), null);
        assertEquals(null, store().loadState(a).orElseThrow().data());
        String b = "order:" + p + "text";
        store().upsertState(new SmStateRow(b, "order", "NEW", "plain text, not JSON", 1, "e1", now()), null);
        assertJsonEquals("\"plain text, not JSON\"", store().loadState(b).orElseThrow().data());
    }

    @Test
    void queriesInsideJsonDataByPath() {
        store().upsertState(new SmStateRow("order:" + p + "q1", "order", "PAID", ORDER_JSON, 1, "e1", now()), null);
        store().upsertState(new SmStateRow("order:" + p + "q2", "order", "NEW",
                ORDER_JSON.replace("\"Ada\"", "\"Grace\"").replace("42", "7"), 1, "e1", now()), null);
        store().upsertState(new SmStateRow("order:" + p + "q3", "order", "NEW", null, 1, "e1", now()), null);
        assertEquals(List.of("order:" + p + "q1"), ids(store().scanStatesWhere("order:" + p, "customer.name", "Ada")));
        assertEquals(List.of("order:" + p + "q2"), ids(store().scanStatesWhere("order:" + p, "customer.loyaltyPoints", "7")));
        assertEquals(2, store().scanStatesWhere("order:" + p, "status", "PAID").size());
        assertEquals(2, store().scanStatesWhere("order:" + p, "total.currency", "EUR").size());
        assertTrue(store().scanStatesWhere("order:" + p, "customer.missing", "x").isEmpty());
    }

    private static List<String> ids(List<SmStateRow> rows) {
        return rows.stream().map(SmStateRow::workflowId).sorted().toList();
    }

    private static void assertJsonEquals(String expected, String actual) {
        assertEquals(JsonData.parse(expected), JsonData.parse(actual), () -> "expected " + expected + " but was " + actual);
    }

    // ---------------------------------------------------------------- traces

    @Test
    void tracesQueryByEventAndWorkflow() {
        long t = now();
        String wf = "order:" + p + "w";
        store().appendTrace(List.of(
                new TraceRow(p + "e1", t, Stage.RECEIVED, wf, null, "r"),
                new TraceRow(p + "e1", t + 5, Stage.LOCK_GRANTED, wf, "account:1", "waitedMs=1"),
                new TraceRow(p + "e1", t + 9, Stage.DONE, wf, null, "accepted e2eMs=9 srcMs=20"),
                new TraceRow(p + "e2", t + 20, Stage.DONE, wf, null, "accepted")));
        List<TraceRow> e1 = store().traceForEvent(p + "e1");
        assertEquals(List.of(Stage.RECEIVED, Stage.LOCK_GRANTED, Stage.DONE), e1.stream().map(TraceRow::stage).toList());
        assertEquals("account:1", e1.get(1).lockKey());
        List<TraceRow> byWf = store().traceForWorkflow(wf, 2);
        assertEquals(2, byWf.size());
        assertEquals(p + "e2", byWf.get(0).eventId(), "newest first");
        assertEquals(2, store().scanTraces(p, Stage.DONE).size());
    }

    @Test
    void manyTraceRowsInOneAppend() {
        List<TraceRow> rows = new ArrayList<>();
        for (int i = 0; i < 1200; i++) {
            rows.add(new TraceRow(p + "bulk", now() + i, Stage.LOCK_WAIT, null, "k" + i, null));
        }
        store().appendTrace(rows);
        assertEquals(1200, store().scanTraces(p + "bulk", Stage.LOCK_WAIT).size());
    }

    @Test
    void recentEventsAreTerminalAndNewestFirst() {
        long t = now() + 60_000; // newer than rows other tests wrote
        store().appendTrace(List.of(
                new TraceRow(p + "r1", t, Stage.DONE, "x:1", null, null),
                new TraceRow(p + "r2", t + 1, Stage.RECEIVED, "x:1", null, null),
                new TraceRow(p + "r3", t + 2, Stage.DROPPED, "x:1", null, "duplicate")));
        List<TraceRow> recent = store().recentEvents(2);
        assertEquals(List.of(p + "r3", p + "r1"), recent.stream().map(TraceRow::eventId).toList());
        assertFalse(recent.stream().anyMatch(r -> r.stage() == Stage.RECEIVED));
    }

    // ---------------------------------------------------------------- helpers

    private static long now() {
        return System.currentTimeMillis();
    }

    private static SmStateRow row(String wf, String state, long version) {
        return new SmStateRow(wf, wf.substring(0, wf.indexOf(':')), state, "{\"v\":" + version + "}", version, "e" + version, now());
    }
}
