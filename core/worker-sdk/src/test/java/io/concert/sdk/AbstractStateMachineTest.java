package io.concert.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.api.SmStateRow;
import io.concert.common.api.TransitionCommit;
import io.concert.common.api.TransitionResult;
import io.concert.store.InMemoryStateStore;
import io.concert.store.StateStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Events applied as the worker applies them (TransitionActivitiesImpl: load, decide, one transaction). */
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
        protected String applyData(String fromState, String toState, String currentData, EventEnvelope event) {
            return "{\"at\":" + now() + "}";
        }
    }

    private final InMemoryStateStore store = new InMemoryStateStore();

    private static EventEnvelope event(String id, String type) {
        return new EventEnvelope(id, "door", "d1", type, List.of(), null, 0, 0);
    }

    private TransitionResult send(StateStore s, String id, String type) {
        return new TransitionActivitiesImpl(s, Map.of("door", DoorMachine.class), null, null, null)
                .applyTransition(event(id, type), Map.of());
    }

    private TransitionResult send(String id, String type) {
        return send(store, id, type);
    }

    @Test
    void appliesTransitionsInOrderFromTheStoredState() {
        assertEquals("OPEN", send("e1", "open").toState());
        assertEquals("CLOSED", send("e2", "close").toState());
        TransitionResult rejected = send("e3", "close");
        assertFalse(rejected.accepted());
        assertEquals("no transition from CLOSED on close", rejected.message());
        TransitionResult r = send("e4", "open");
        assertTrue(r.accepted());
        assertEquals(3, r.version());
        assertEquals("OPEN", store.loadState("door:d1").orElseThrow().state());
        assertEquals(3, store.loadState("door:d1").orElseThrow().version());
    }

    @Test
    void aRedeliveredEventGetsItsStoredResultAndChangesNothing() {
        TransitionResult first = send("same", "open");
        send("e2", "close");

        TransitionResult again = send("same", "open"); // on the current state (CLOSED) it would apply again

        assertEquals(first, again);
        assertEquals(2, store.loadState("door:d1").orElseThrow().version());
        assertEquals("CLOSED", store.loadState("door:d1").orElseThrow().state());
    }

    @Test
    void anEntityChangedAfterTheLoadIsDecidedAgainOnTheStoredRow() {
        send("e1", "open");
        // a store that, on the first commit, lets another writer move the entity first (as if it raced)
        AtomicInteger commits = new AtomicInteger();
        StateStore racing = proxy((method, args) -> {
            if (method.equals("commitTransition") && commits.incrementAndGet() == 1) {
                store.commitTransition(new TransitionCommit("door:d1", "x1", 1,
                        new TransitionResult("x1", "OPEN", "CLOSED", true, null, 2),
                        new SmStateRow("door:d1", "door", "CLOSED", null, 2, "x1", 0), List.of(), 0));
            }
            return null;
        });

        TransitionResult r = send(racing, "e2", "open"); // decided on OPEN it would be rejected; on CLOSED it applies

        assertTrue(r.accepted(), r.toString());
        assertEquals("CLOSED", r.fromState());
        assertEquals(3, r.version());
        assertEquals(2, commits.get(), "the conflicting commit, then the commit decided on the stored row");
    }

    @Test
    void aStoreFailureCommitsNothingAndTheRetryApplies() {
        StateStore down = proxy((method, args) -> {
            if (method.equals("commitTransition")) {
                throw new IllegalStateException("database unreachable");
            }
            return null;
        });

        assertThrows(IllegalStateException.class, () -> send(down, "e1", "open")); // the activity fails: Temporal retries
        assertTrue(store.loadState("door:d1").isEmpty());

        assertEquals(1, send("e1", "open").version()); // the retry, once the store is back
    }

    @Test
    void theDecisionUsesOneTimestamp() {
        send("e1", "open");
        assertTrue(store.loadState("door:d1").orElseThrow().data().matches("\\{\"at\":\\d{13}}"));
    }

    @Test
    void mermaidHighlightsCurrentState() {
        String m = DoorMachine.SPEC.toMermaid("OPEN");
        assertTrue(m.contains("CLOSED --> OPEN : open"));
        assertTrue(m.contains("class OPEN current"));
    }

    private interface Hook {
        Object before(String method, Object[] args) throws Exception;
    }

    /** The in-memory store, with {@code hook} run before every call. */
    private StateStore proxy(Hook hook) {
        return (StateStore) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {StateStore.class},
                (p, method, args) -> {
                    hook.before(method.getName(), args);
                    try {
                        return method.invoke(store, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
