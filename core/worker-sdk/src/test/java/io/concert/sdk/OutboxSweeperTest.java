package io.concert.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.api.SmStateRow;
import io.concert.common.api.TransitionCommit;
import io.concert.common.api.TransitionResult;
import io.concert.sdk.events.EnqueueRequest;
import io.concert.sdk.events.EventEnqueueActivities;
import io.concert.store.InMemoryStateStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Outbox rows the fast path did not deliver (the worker died after commit) are delivered by the sweeper. */
class OutboxSweeperTest {

    private final InMemoryStateStore store = new InMemoryStateStore();
    private final List<String> delivered = new ArrayList<>();
    private final EventEnqueueActivities enqueue = new EventEnqueueActivities() {
        @Override
        public void enqueue(EnqueueRequest request) {}

        @Override
        public void deliverOutbox(List<EventEnvelope> events, String emittedBy) {
            events.forEach(e -> delivered.add(emittedBy + ">" + e.eventId()));
            store.markOutboxSent(events.stream().map(EventEnvelope::eventId).toList());
        }

        @Override
        public void unblock(String lockKey, String requestId) {}
    };

    private void commit(String wf, String eventId, String... children) {
        List<EventEnvelope> out = new ArrayList<>();
        EventEnvelope parent = new EventEnvelope(eventId, "order", wf.substring(6), "pay", List.of(), null, 0, 0);
        for (String c : children) {
            out.add(new EventEnvelope(c, "invoice", "i1", "issue", List.of(), null, 0, 0).withParent(parent));
        }
        store.commitTransition(new TransitionCommit(wf, eventId, 0, new TransitionResult(eventId, "NEW", "PAID", true, null, 1),
                new SmStateRow(wf, "order", "PAID", null, 1, eventId, 0), out, 1000));
    }

    @Test
    void deliversPendingRowsOfItsTypeOnceGroupedByTheirParent() {
        commit("order:o1", "e1", "e1.1", "e1.2");
        commit("order:o2", "e2", "e2.1");
        commit("other:o3", "e3", "e3.1"); // another type: not this sweeper's

        assertEquals(3, OutboxSweeper.sweepOnce(store, "order", enqueue, 2000));
        assertEquals(List.of("e1>e1.1", "e1>e1.2", "e2>e2.1"), delivered);
        assertTrue(store.outboxSent("e1.1"));
        assertEquals(0, OutboxSweeper.sweepOnce(store, "order", enqueue, 2000), "sent rows are not delivered again");
    }

    @Test
    void leavesRowsNewerThanTheCutOffToTheFastPath() {
        commit("order:o1", "e1", "e1.1");
        assertEquals(0, OutboxSweeper.sweepOnce(store, "order", enqueue, 500));
        assertTrue(delivered.isEmpty());
    }
}
