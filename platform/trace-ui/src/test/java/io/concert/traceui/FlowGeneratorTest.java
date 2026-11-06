package io.concert.traceui;

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
    private final FlowGenerator gen = new FlowGenerator(null, "test-stream");

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
        TraceUiMain.registerSpecs();
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
    void instancesCarryLocksAndInjectInvalidEvents() {
        FlowGenerator.Run claim = gen.new Run("claim", "samples", 1, 10, true);
        List<EventEnvelope> events = claim.instance(1, false, 1000);
        assertTrue(events.size() >= 3);
        EventEnvelope first = events.getFirst();
        assertTrue(first.instanceKey().startsWith("CLAIM-1001-"), first.instanceKey());
        assertEquals(1000 + FlowGenerator.STEP_MS, events.get(1).sourceTsMillis(), "a step apart");
        List<EventEnvelope> bad = claim.instance(2, true, 1000);
        assertEquals(1, bad.size(), "no error samples for claim: the last event alone, invalid in the initial state");
        assertEquals(events.getLast().eventType(), bad.getFirst().eventType());

        FlowGenerator.Run order = gen.new Run("order", "walk", 1, 10, true);
        assertEquals(1, order.instance(5, true, 0).size());
        assertTrue(order.instance(1, false, 0).size() >= 2);

        FlowGenerator.Run ev = gen.new Run("ev:orders", "samples", 1, 10, false);
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
}
