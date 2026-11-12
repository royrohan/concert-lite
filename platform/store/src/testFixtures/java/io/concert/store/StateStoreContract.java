package io.concert.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.TraceRow;
import io.concert.common.api.CommitOutcome;
import io.concert.common.api.TransitionCommit;
import io.concert.common.api.TransitionResult;
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

    // ---------------------------------------------------------------- DB-truth transitions

    /** Stores that hold entity state (postgres, dsql, memory); others skip these tests. */
    protected boolean holdsEntityState() {
        return true;
    }

    private TransitionCommit accept(String wf, String event, long from, String fromState, String toState, String... emitted) {
        long v = from + 1;
        List<EventEnvelope> out = new ArrayList<>();
        for (String id : emitted) {
            out.add(new EventEnvelope(id, "child", "c1", "go", List.of(), null, now(), 0));
        }
        return new TransitionCommit(wf, event, from, new TransitionResult(event, fromState, toState, true, null, v),
                new SmStateRow(wf, "order", toState, "{\"n\":" + v + "}", v, event, now()), out, now());
    }

    private TransitionCommit reject(String wf, String event, long at, String state) {
        return new TransitionCommit(wf, event, at, new TransitionResult(event, state, state, false, "no", at), null, List.of(), now());
    }

    @Test
    void anAcceptedTransitionCommitsStateAppliedEventAndOutboxTogether() {
        org.junit.jupiter.api.Assumptions.assumeTrue(holdsEntityState());
        String wf = "order:" + p + "1";
        CommitOutcome o = store().commitTransition(accept(wf, p + "e1", 0, "NEW", "PAID", p + "e1.1", p + "e1.2"));

        assertEquals(CommitOutcome.Status.COMMITTED, o.status());
        SmStateRow row = store().loadEntity(wf, p + "e1").row();
        assertEquals("PAID", row.state());
        assertEquals(1, row.version());
        assertTrue(store().loadEntity(wf, p + "e1").applied().accepted());
        assertEquals(List.of(p + "e1.1", p + "e1.2"),
                store().pendingOutbox(wf, now() + 1000, 10).stream().map(EventEnvelope::eventId).toList());
    }

    @Test
    void aRedeliveredEventGetsItsStoredResultAndWritesNothing() {
        org.junit.jupiter.api.Assumptions.assumeTrue(holdsEntityState());
        String wf = "order:" + p + "2";
        store().commitTransition(accept(wf, p + "e1", 0, "NEW", "PAID"));
        store().commitTransition(accept(wf, p + "e2", 1, "PAID", "SHIPPED"));

        // e1 again, decided on stale state: refused with what was recorded the first time
        CommitOutcome again = store().commitTransition(accept(wf, p + "e1", 2, "SHIPPED", "OTHER", p + "e1.9"));

        assertEquals(CommitOutcome.Status.ALREADY_APPLIED, again.status());
        assertEquals("PAID", again.result().toState());
        assertEquals(1, again.result().version());
        assertEquals("SHIPPED", store().loadEntity(wf, null).row().state());
        assertTrue(store().pendingOutbox(wf, now() + 1000, 10).isEmpty(), "nothing emitted by the redelivery");
    }

    @Test
    void aTransitionDecidedOnAStaleVersionConflictsAndWritesNothing() {
        org.junit.jupiter.api.Assumptions.assumeTrue(holdsEntityState());
        String wf = "order:" + p + "3";
        store().commitTransition(accept(wf, p + "e1", 0, "NEW", "PAID"));

        CommitOutcome stale = store().commitTransition(accept(wf, p + "e2", 0, "NEW", "PAID", p + "e2.1"));

        assertEquals(CommitOutcome.Status.CONFLICT, stale.status());
        assertEquals(1, stale.current().version());
        assertNull(store().loadEntity(wf, p + "e2").applied(), "the event is not recorded as applied");
        assertTrue(store().pendingOutbox(wf, now() + 1000, 10).isEmpty());
        // decided again on the current version, it commits
        assertEquals(CommitOutcome.Status.COMMITTED, store().commitTransition(accept(wf, p + "e2", 1, "PAID", "SHIPPED")).status());
    }

    @Test
    void aRejectionIsRecordedWithoutChangingStateAndOnlyOnTheVersionItWasDecidedOn() {
        org.junit.jupiter.api.Assumptions.assumeTrue(holdsEntityState());
        String wf = "order:" + p + "4";
        assertEquals(CommitOutcome.Status.COMMITTED, store().commitTransition(reject(wf, p + "r0", 0, "NEW")).status());
        assertNull(store().loadEntity(wf, null).row(), "a rejected first event creates no entity row");
        store().commitTransition(accept(wf, p + "e1", 0, "NEW", "PAID"));

        assertEquals(CommitOutcome.Status.CONFLICT, store().commitTransition(reject(wf, p + "r1", 0, "NEW")).status());
        assertEquals(CommitOutcome.Status.COMMITTED, store().commitTransition(reject(wf, p + "r1", 1, "PAID")).status());
        assertFalse(store().loadEntity(wf, p + "r1").applied().accepted());
        assertEquals(1, store().loadEntity(wf, null).row().version());
    }

    @Test
    void ofConcurrentCommitsFromOneVersionExactlyOneWins() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(holdsEntityState());
        String wf = "order:" + p + "5";
        store().commitTransition(accept(wf, p + "e0", 0, "NEW", "OPEN"));
        List<Future<CommitOutcome>> fs = new ArrayList<>();
        try (var pool = Executors.newFixedThreadPool(8)) {
            for (int i = 0; i < 8; i++) {
                String event = p + "c" + i;
                fs.add(pool.submit(() -> store().commitTransition(accept(wf, event, 1, "OPEN", "S" + event, event + ".1"))));
            }
        }
        long committed = 0;
        for (Future<CommitOutcome> f : fs) {
            if (f.get().status() == CommitOutcome.Status.COMMITTED) {
                committed++;
            }
        }
        assertEquals(1, committed);
        assertEquals(2, store().loadEntity(wf, null).row().version());
        assertEquals(1, store().pendingOutbox(wf, now() + 1000, 10).size(), "only the winner's outbox row exists");
    }

    @Test
    void outboxRowsAreDeliveredOnceMarkedSentAndSweptLater() {
        org.junit.jupiter.api.Assumptions.assumeTrue(holdsEntityState());
        String wf = "order:" + p + "6";
        store().commitTransition(accept(wf, p + "e1", 0, "NEW", "PAID", p + "e1.1", p + "e1.2"));
        assertTrue(store().pendingOutbox(wf, now() - 60_000, 10).isEmpty(), "only rows created before the cut-off");

        store().markOutboxSent(List.of(p + "e1.1"));
        store().markOutboxSent(List.of(p + "e1.1")); // idempotent

        assertEquals(List.of(p + "e1.2"), store().pendingOutbox(wf, now() + 1000, 10).stream().map(EventEnvelope::eventId).toList());
        store().markOutboxSent(List.of(p + "e1.2"));
        assertTrue(store().sweepEntityLog(now() + 1000, now() + 1000, 100) >= 3, "two sent rows and the applied record");
        assertNull(store().loadEntity(wf, p + "e1").applied());
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

    @Test
    void purgesAFlowsRowsOnly() {
        String type = p.replace("-", "_") + "flowtype";
        String wf = type + ":K1";
        store().upsertState(new SmStateRow(wf, type, "OPEN", null, 1, "e1", now()),
                new TraceRow(p + "pe", now(), Stage.TRANSITION, wf, null, "NEW -> OPEN"));
        store().upsertState(row("ledger:" + p + "keep", "OPEN", 1), null);
        store().saveCheckpoint("concert-" + p + "flow", "shard-0", "42");
        long n;
        try {
            n = store().purgeFlow(java.util.Set.of(type), List.of(type + ":"), "concert-" + p + "flow");
        } catch (UnsupportedOperationException e) {
            org.junit.jupiter.api.Assumptions.abort(e.getMessage());
            return;
        }
        assertTrue(n >= 3, "deleted " + n);
        assertTrue(store().loadState(wf).isEmpty());
        assertTrue(store().traceForEvent(p + "pe").isEmpty());
        assertTrue(store().loadCheckpoint("concert-" + p + "flow", "shard-0").isEmpty());
        assertTrue(store().loadState("ledger:" + p + "keep").isPresent());
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

    @Test
    void tracesSinceIsAWindowNewestFirst() {
        long t = now() + 120_000; // newer than rows other tests wrote
        store().appendTrace(List.of(
                new TraceRow(p + "w0", t - 10_000, Stage.DONE, "x:1", null, null),
                new TraceRow(p + "w1", t, Stage.DONE, "x:1", null, null),
                new TraceRow(p + "w2", t + 1, Stage.RECEIVED, "x:1", null, null),
                new TraceRow(p + "w3", t + 2, Stage.FAILED, "x:1", null, "boom")));
        List<String> ids = store().tracesSince(t, 100).stream().map(TraceRow::eventId)
                .filter(id -> id.startsWith(p)).filter(id -> !id.equals(p + "w2")).toList();
        // RECEIVED (w2) is served by stores that index every stage by time; terminal rows by all.
        assertEquals(List.of(p + "w3", p + "w1"), ids);
    }

    // ---------------------------------------------------------------- helpers

    private static long now() {
        return System.currentTimeMillis();
    }

    private static SmStateRow row(String wf, String state, long version) {
        return new SmStateRow(wf, wf.substring(0, wf.indexOf(':')), state, "{\"v\":" + version + "}", version, "e" + version, now());
    }
}
