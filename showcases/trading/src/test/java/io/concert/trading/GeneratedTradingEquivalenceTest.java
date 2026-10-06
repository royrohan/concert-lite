package io.concert.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.concert.eco.trading_gen.TradingGenMachines;
import io.concert.sdk.MachineCatalog;
import io.concert.sdk.StateMachineSpec;
import io.concert.trading.machines.TradingSpecs;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The trading machines declared with the concert::sm profile (examples/trading-gen, generated into
 * showcases/trading-gen by ./generate-concert-ecosystem) are the hand-written {@link TradingFlows}: same
 * states, transitions (in order), payload classes, terminal states and lock templates. The generated smTypes
 * carry a {@code gen_} prefix (so the two never clash), and lock namespaces naming a trading smType follow it.
 */
class GeneratedTradingEquivalenceTest {

    private static String gen(String smType) {
        return "gen_" + smType;
    }

    private static String namespace(String ns) {
        return TradingFlows.all().stream().anyMatch(f -> f.smType().equals(ns)) ? gen(ns) : ns;
    }

    private static List<String> edges(StateMachineSpec spec) {
        List<String> out = new ArrayList<>();
        for (StateMachineSpec.Edge e : spec.edges()) {
            out.add(e.from() + " -" + e.eventType() + "-> " + e.to() + " : " + e.payloadType().getSimpleName());
        }
        return out;
    }

    @Test
    void generatedSpecsEqualTradingFlows() {
        assertEquals(TradingFlows.all().stream().map(f -> gen(f.smType())).toList(), List.copyOf(TradingGenMachines.ALL.keySet()));
        for (TradingFlows.Flow flow : TradingFlows.all()) {
            MachineCatalog.Machine generated = TradingGenMachines.get(gen(flow.smType()));
            StateMachineSpec expected = TradingSpecs.spec(flow);
            StateMachineSpec actual = generated.spec();
            String type = flow.smType();
            assertEquals(expected.initialState(), actual.initialState(), type);
            assertEquals(List.copyOf(expected.states()), List.copyOf(actual.states()), type + " states");
            assertEquals(List.copyOf(flow.terminalStates()), List.copyOf(actual.terminalStates()), type + " terminal states");
            assertEquals(edges(expected), edges(actual), type + " transitions and payload classes");
            List<String> expectedTransitions = flow.transitions().stream()
                    .map(t -> t.from() + " -" + t.eventType() + "-> " + t.to() + " : " + t.payloadClass()).toList();
            assertEquals(expectedTransitions, edges(actual), type + " transitions vs TradingFlows");

            Map<String, List<String>> locks = TradingGenMachines.LOCKS.get(gen(type));
            assertEquals(List.copyOf(flow.eventTypes()).stream().sorted().toList(), locks.keySet().stream().sorted().toList(), type + " events");
            for (String event : flow.eventTypes()) {
                List<String> expectedLocks = flow.locksFor(event).stream()
                        .map(k -> namespace(k.namespace()) + ":{" + k.payloadField() + "}").toList();
                assertEquals(expectedLocks, locks.get(event), type + "." + event + " lock templates");
            }
        }
    }

    private static final Map<String, String> SPEC_CLASSES = Map.of(TradingFlows.ORDER, "GenTradingOrderSpec",
            TradingFlows.EXECUTION, "GenTradingExecutionSpec", TradingFlows.FILL, "GenTradingFillSpec",
            TradingFlows.ALLOCATION, "GenTradingAllocationSpec");

    @Test
    void generatedAggregatesAreTheTradingAggregates() throws Exception {
        for (TradingFlows.Flow flow : TradingFlows.all()) {
            Class<?> spec = Class.forName("io.concert.eco.trading_gen." + SPEC_CLASSES.get(flow.smType()));
            assertEquals(TradingFlows.ORDER_PACKAGE + flow.modelClass(), spec.getField("ROOT_CLASS").get(null), flow.smType());
            assertEquals(flow.keyField(), spec.getField("ID_PROPERTY").get(null), flow.smType());
            assertEquals("status", spec.getField("STATUS_PROPERTY").get(null), flow.smType());
        }
    }
}
