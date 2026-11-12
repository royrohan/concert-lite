package io.concert.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.api.TransitionResult;
import io.concert.sdk.ModelStateMachineTest.TicketMachine;
import io.concert.sink.EntitySnapshot;
import io.concert.store.InMemoryStateStore;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The completed-entity snapshot of {@link ModelStateMachine} (ticket machine: CLOSED is terminal), published after the
 * terminal transition commits. A failed publish is logged and does not fail the committed transition (the
 * reconciliation job republishes what the sinks miss).
 */
class EntitySnapshotPublishTest {

    /** Captures snapshots; fails the first {@code failures} attempts. */
    static final class FakePublisher implements EntitySnapshotPublisher {
        final List<EntitySnapshot> published = new CopyOnWriteArrayList<>();
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();

        @Override
        public void publish(EntitySnapshot snapshot) {
            attempts.incrementAndGet();
            if (failures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IllegalStateException("kafka down");
            }
            published.add(snapshot);
        }
    }

    private InMemoryStateStore store;
    private MachineHarness harness;
    private final FakePublisher publisher = new FakePublisher();

    @BeforeEach
    void setUp() {
        store = new InMemoryStateStore();
        harness = new MachineHarness(store).register("ticket", TicketMachine.class).snapshots(publisher);
    }

    private TransitionResult send(String id, String type, String payload) {
        return harness.apply(new EventEnvelope(id, "ticket", "t1", type, List.of(), payload, 0, 0));
    }

    private void closeTicket() {
        assertTrue(send("e1", "assign", "{\"assignee\":\"ann\"}").accepted());
        assertTrue(send("e2", "resolve", "{\"resolution\":\"fixed\"}").accepted());
        assertTrue(send("e3", "close", null).accepted());
    }

    @Test
    void terminalTransitionPublishesOneSnapshot() {
        closeTicket();

        assertEquals(1, publisher.published.size());
        EntitySnapshot s = publisher.published.getFirst();
        assertEquals("ticket:t1", s.entityId());
        assertEquals("ticket", s.smType());
        assertEquals("CLOSED", s.state());
        assertEquals(3, s.version());
        assertNotNull(s.completedAt());
        assertEquals(store.loadState("ticket:t1").orElseThrow().data(), s.modelJson());

        // a rejected event in the terminal state, and a redelivery of the terminal event, publish nothing more
        assertFalse(send("e4", "close", null).accepted());
        assertTrue(send("e3", "close", null).accepted());
        assertEquals(1, publisher.attempts.get());
    }

    @Test
    void nonTerminalTransitionsDoNotPublish() {
        assertTrue(send("e1", "assign", "{\"assignee\":\"ann\"}").accepted());
        assertTrue(send("e2", "resolve", "{\"resolution\":\"fixed\"}").accepted());
        assertTrue(publisher.published.isEmpty());
    }

    @Test
    void aFailedPublishDoesNotFailTheCommittedTransition() {
        publisher.failures.set(1);

        closeTicket();

        assertEquals("CLOSED", store.loadState("ticket:t1").orElseThrow().state());
        assertEquals(1, publisher.attempts.get());
        assertTrue(publisher.published.isEmpty(), "left to reconciliation");
    }
}
