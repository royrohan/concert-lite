package io.concert.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.sdk.StateMachineRegistry;
import io.concert.sdk.StateMachineSpec;
import java.util.List;
import org.junit.jupiter.api.Test;

/** How the flow view's generator makes events per flow (no Kinesis: the events, not their publishing). */
class FlowGeneratorTest {
    private final FlowGenerator gen = new FlowGenerator(null, f -> "test-stream");

    @Test
    void strategyPerFlow() {
        assertEquals("walk", gen.strategy("order"));
        assertEquals("samples", gen.strategy("claim"));
        assertEquals("samples", gen.strategy("gen_trading_fill"));
        assertEquals("trading", gen.strategy("trading_order"));
        assertEquals("samples", gen.strategy("ev:orders"));
        assertNull(gen.strategy("nope"));
    }

    @Test
    void walkGoesFromInitialToATerminalState() {
        DashboardMain.registerSpecs();
        StateMachineSpec order = StateMachineRegistry.get("order");
        List<StateMachineSpec.Edge> path = FlowGenerator.walk(order);
        assertEquals(order.initialState(), path.getFirst().from());
        assertTrue(order.isTerminal(path.getLast().to()), "ends terminal: " + path);
        List<List<StateMachineSpec.Edge>> all = FlowGenerator.paths(order);
        assertTrue(all.stream().anyMatch(p -> p.getLast().to().equals("DELIVERED")), "happy path among them");
        assertTrue(all.stream().anyMatch(p -> p.getLast().to().equals("CANCELLED")), "cancellations too");
    }

    @Test
    void samplesAreRewrittenConsistentlyPerInstance() {
        FlowGenerator.Sample happy = gen.samplesFor("ev:orders", false).stream()
                .filter(s -> s.name().equals("order-happy-path")).findFirst().orElseThrow();
        String text = Json.write(gen.rewrite(happy, "r1-7"));
        assertFalse(text.contains("\"ORDER-HAPPY-1\""));
        assertTrue(text.contains("\"ORDER-HAPPY-1-r1-7\""));
        // A sku is a lock key of RestockEvent: renamed everywhere, including the order's lines, so stock still matches.
        assertTrue(text.contains("\"SKU-RED-r1-7\""));
        assertFalse(text.contains("\"SKU-RED\""));
        assertTrue(gen.samplesFor("ev:orders", true).stream().anyMatch(s -> s.name().equals("payment-poison")));
    }

    @Test
    void entityLockFieldsAreRewrittenSoInstancesDontShareALock() {
        FlowGenerator.Sample flow = gen.samplesFor("gen_trading_execution", false).getFirst();
        String text = Json.write(gen.rewrite(flow, "r2-1"));
        // the execution's lock key gen_trading_order:{orderId}: each generated instance gets its own order id
        assertTrue(text.contains("\"GEN-TRADING-ORDER-1001-r2-1\""), text);
        assertFalse(text.contains("\"GEN-TRADING-ORDER-1001\""), text);
    }

    @Test
    void instancesCarryLocksAndInjectInvalidEvents() {
        FlowGenerator.Run claim = gen.new Run("claim", "samples", 1, 10, true, false);
        List<EventEnvelope> events = claim.instance(1, false, 1000);
        assertTrue(events.size() >= 3);
        EventEnvelope first = events.getFirst();
        assertTrue(first.instanceKey().startsWith("CLAIM-1001-"), first.instanceKey());
        assertEquals(1000 + FlowGenerator.STEP_MS, events.get(1).sourceTsMillis(), "a step apart");
        List<EventEnvelope> bad = claim.instance(2, true, 1000);
        assertEquals(1, bad.size(), "no error samples for claim: the last event alone, invalid in the initial state");
        assertEquals(events.getLast().eventType(), bad.getFirst().eventType());

        FlowGenerator.Run order = gen.new Run("order", "walk", 1, 10, true, false);
        assertEquals(1, order.instance(5, true, 0).size());
        assertTrue(order.instance(1, false, 0).size() >= 2);

        FlowGenerator.Run ev = gen.new Run("ev:orders", "samples", 1, 10, false, false);
        EventEnvelope create = ev.instance(1, false, 0).stream().filter(e -> e.eventType().equals("OrderCreateEvent"))
                .findAny().orElse(null);
        if (create != null) {
            assertEquals("event", create.style());
            assertTrue(create.lockKeys().stream().anyMatch(k -> k.startsWith("order:")), create.lockKeys().toString());
        }
    }

    @Test
    void failureMessagesGroupAcrossEvents() {
        assertEquals(FlowMetrics.normalize("payment 42 for ORDER-17 declined (attempt 3)"),
                FlowMetrics.normalize("payment 99 for ORDER-301 declined (attempt 1)"));
        assertEquals("(no detail)", FlowMetrics.normalize(null));
    }

    @Test
    void entityKeyOnlyDropsDeclaredLockKeysOfEntityEventsOnly() {
        io.concert.common.EventEnvelope order = new io.concert.common.EventEnvelope("e1", "rs_trading_order", "O-1", "submit",
                java.util.List.of("account:A-1"), "{}", 1, 0);
        io.concert.common.EventEnvelope stripped = FlowGenerator.withoutLockKeys(order);
        assertEquals(java.util.List.of(), stripped.lockKeys());
        assertEquals(java.util.List.of("rs_trading_order:O-1"), stripped.effectiveLockKeys(), "serialized on its entity alone");
        io.concert.common.EventEnvelope tick = io.concert.common.EventEnvelope.event("t1", "ticks", "Tick",
                java.util.List.of("sym:AAPL"), "{}", 1);
        assertEquals(java.util.List.of("sym:AAPL"), FlowGenerator.withoutLockKeys(tick).lockKeys(), "event style keeps its keys");
    }
}
