package io.concert.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.trading.TradingFlows.Flow;
import io.concert.trading.TradingFlows.LockKey;
import io.concert.trading.TradingFlows.Transition;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class TradingFlowsTest {

    private static Set<String> reachable(Flow f) {
        Set<String> seen = new HashSet<>(Set.of(f.initialState()));
        Deque<String> todo = new ArrayDeque<>(seen);
        while (!todo.isEmpty()) {
            String s = todo.pop();
            f.transitions().stream().filter(t -> t.from().equals(s)).map(Transition::to).filter(seen::add).forEach(todo::push);
        }
        return seen;
    }

    @Test
    void everyStateAndEventTypeIsReachable() {
        for (Flow f : TradingFlows.all()) {
            Set<String> reach = reachable(f);
            assertEquals(f.states(), reach, f.smType());
            Set<String> firable = f.transitions().stream().filter(t -> reach.contains(t.from())).map(Transition::eventType)
                    .collect(Collectors.toSet());
            assertEquals(f.eventTypes(), firable, f.smType());
        }
    }

    @Test
    void terminalsAreTheExpectedStatesAndEveryStateCanFinish() {
        assertEquals(Set.of("FILLED", "CANCELLED", "REJECTED"), TradingFlows.ORDER_FLOW.terminalStates());
        assertEquals(Set.of("FILLED", "CANCELLED", "REJECTED"), TradingFlows.EXECUTION_FLOW.terminalStates());
        assertEquals(Set.of("BOOKED"), TradingFlows.FILL_FLOW.terminalStates());
        assertEquals(Set.of("CONFIRMED"), TradingFlows.ALLOCATION_FLOW.terminalStates());
        for (Flow f : TradingFlows.all()) {
            for (String terminal : f.terminalStates()) {
                assertTrue(f.transitions().stream().noneMatch(t -> t.from().equals(terminal)), terminal + " has no outgoing");
            }
            for (String s : f.states()) {
                Flow from = new Flow(f.smType(), f.keyField(), f.modelClass(), f.statusEnum(), s, f.transitions(), f.locks());
                assertTrue(reachable(from).stream().anyMatch(f.terminalStates()::contains), s + " can reach a terminal");
            }
        }
    }

    @Test
    void tableIsDeterministicAndPayloadTypesAgreePerEvent() {
        for (Flow f : TradingFlows.all()) {
            Set<String> keys = new HashSet<>();
            for (Transition t : f.transitions()) {
                assertTrue(keys.add(t.from() + "/" + t.eventType()), "duplicate " + t);
                assertEquals(f.payloadClass(t.eventType()), t.payloadClass(), t.toString());
            }
        }
    }

    @Test
    void lockKeysReferenceKnownNamespacesAndCoverEveryEvent() {
        for (Flow f : TradingFlows.all()) {
            assertEquals(f.eventTypes(), f.locks().keySet(), f.smType());
            for (List<LockKey> keys : f.locks().values()) {
                for (LockKey k : keys) {
                    assertTrue(TradingFlows.lockNamespaces().contains(k.namespace()), k.toString());
                    assertFalse(k.namespace().equals(f.smType()), "the target entity key is implied");
                }
            }
        }
        assertEquals(List.of(new LockKey(TradingFlows.ACCOUNT, "accountId")), TradingFlows.ORDER_FLOW.locksFor("submit"));
        assertEquals(Set.of(TradingFlows.ORDER, TradingFlows.EXECUTION),
                TradingFlows.FILL_FLOW.locksFor("book").stream().map(LockKey::namespace).collect(Collectors.toSet()));
        assertEquals(Set.of(TradingFlows.ORDER, TradingFlows.ACCOUNT),
                TradingFlows.ALLOCATION_FLOW.locksFor("allocate").stream().map(LockKey::namespace).collect(Collectors.toSet()));
    }

    /**
     * Per-entity order holds only on the first sorted lock key: every event of an entity must have the
     * same first key, i.e. the same smallest namespace (rendered from the same field).
     */
    @Test
    void allEventsOfAnEntityShareTheirFirstSortedKey() {
        for (Flow f : TradingFlows.all()) {
            Set<String> firsts = new HashSet<>();
            for (String e : f.eventTypes()) {
                String first = f.smType() + ":<" + f.keyField() + ">";
                for (LockKey k : f.locksFor(e)) {
                    if ((k.namespace() + ":").compareTo(first) < 0) {
                        first = k.toString();
                    }
                }
                firsts.add(first);
            }
            assertEquals(1, firsts.size(), f.smType() + " first keys " + firsts);
        }
    }

    @Test
    void mermaidAndLockTableListEverything() {
        String mermaid = TradingFlows.toMermaid();
        for (Flow f : TradingFlows.all()) {
            for (Transition t : f.transitions()) {
                assertTrue(mermaid.contains(t.from() + " --> " + t.to() + " : " + t.eventType()), t.toString());
            }
        }
        assertTrue(TradingFlows.lockTable().contains("| trading_fill:<fillId> | book | FillCommand | trading_order:<orderId>, trading_execution:<execId> |"));
    }
}
