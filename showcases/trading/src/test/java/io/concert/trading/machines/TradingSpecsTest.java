package io.concert.trading.machines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.concert.sdk.StateMachineRegistry;
import io.concert.sdk.StateMachineSpec;
import io.concert.trading.TradingFlows;
import io.concert.trading.order.AllocationStatus;
import io.concert.trading.order.ExecutionStatus;
import io.concert.trading.order.FillStatus;
import io.concert.trading.order.OrderStatus;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** The machines' specs are TradingFlows with typed payloads, and states map onto the status enums. */
class TradingSpecsTest {

    private static final Map<String, Class<? extends Enum<?>>> STATUS = Map.of(
            TradingFlows.ORDER, OrderStatus.class, TradingFlows.EXECUTION, ExecutionStatus.class,
            TradingFlows.FILL, FillStatus.class, TradingFlows.ALLOCATION, AllocationStatus.class);

    @Test
    void specsMirrorTheFlows() {
        assertEquals(TradingFlows.all().stream().map(TradingFlows.Flow::smType).toList(),
                List.copyOf(TradingMachines.ALL.keySet()));
        for (TradingFlows.Flow flow : TradingFlows.all()) {
            StateMachineSpec spec = TradingMachines.get(flow.smType()).spec();
            assertEquals(flow.initialState(), spec.initialState());
            assertEquals(flow.states(), spec.states());
            assertEquals(flow.terminalStates(), spec.terminalStates());
            assertEquals(flow.transitions().size(), spec.edges().size());
            for (TradingFlows.Transition t : flow.transitions()) {
                StateMachineSpec.Edge e = spec.edge(t.from(), t.eventType()).orElseThrow();
                assertEquals(t.to(), e.to());
                assertEquals(t.payloadClass(), e.payloadType().getSimpleName());
            }
            // Every state is a constant of the flow's status enum (machines set status = valueOf(state)).
            Set<String> constants = Arrays.stream(STATUS.get(flow.smType()).getEnumConstants()).map(Enum::name)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            assertEquals(flow.statusEnum(), STATUS.get(flow.smType()).getSimpleName());
            assertEquals(constants, new LinkedHashSet<>(flow.states()));
        }
    }

    @Test
    void unknownPayloadClassFails() {
        assertThrows(NoSuchElementException.class, () -> TradingSpecs.commandClass("NoSuchCommand"));
    }

    @Test
    void registryIsComplete() {
        TradingMachines.registerSpecs();
        TradingMachines.ALL.forEach((type, m) -> assertEquals(m.spec(), StateMachineRegistry.get(type)));
        assertThrows(IllegalArgumentException.class, () -> TradingMachines.get("order"));
    }
}
