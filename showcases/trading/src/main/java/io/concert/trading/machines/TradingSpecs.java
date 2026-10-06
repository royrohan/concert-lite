package io.concert.trading.machines;

import io.concert.sdk.StateMachineSpec;
import io.concert.trading.TradingFlows;
import io.concert.trading.command.AckCommand;
import io.concert.trading.command.AllocateCommand;
import io.concert.trading.command.CancelCommand;
import io.concert.trading.command.CloseOrderCommand;
import io.concert.trading.command.ConfirmAllocationCommand;
import io.concert.trading.command.FillCommand;
import io.concert.trading.command.NewOrderCommand;
import io.concert.trading.command.OrderEvent;
import io.concert.trading.command.RejectCommand;
import io.concert.trading.command.RouteCommand;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Builds the SDK's {@link StateMachineSpec} from a {@link TradingFlows.Flow}, so the transition tables
 * exist once: states, transitions and terminal states come from the flow, and each transition's
 * payload class name ({@code commands.pure}) is resolved to its generated class.
 */
public final class TradingSpecs {
    private TradingSpecs() {}

    /** Generated command classes by simple name (= the Pure class name in {@code trading::command}). */
    private static final Map<String, Class<? extends OrderEvent>> COMMANDS = index(List.of(
            NewOrderCommand.class, AckCommand.class, RejectCommand.class, RouteCommand.class, FillCommand.class,
            CancelCommand.class, AllocateCommand.class, ConfirmAllocationCommand.class, CloseOrderCommand.class));

    private static Map<String, Class<? extends OrderEvent>> index(List<Class<? extends OrderEvent>> classes) {
        Map<String, Class<? extends OrderEvent>> m = new LinkedHashMap<>();
        classes.forEach(c -> m.put(c.getSimpleName(), c));
        return Map.copyOf(m);
    }

    /** The generated class of a payload class named in {@link TradingFlows}. */
    public static Class<? extends OrderEvent> commandClass(String payloadClass) {
        Class<? extends OrderEvent> c = COMMANDS.get(payloadClass);
        if (c == null) {
            throw new NoSuchElementException("no generated command class for " + TradingFlows.COMMAND_PACKAGE + payloadClass);
        }
        return c;
    }

    /** The flow's transition table with typed payloads. */
    public static StateMachineSpec spec(TradingFlows.Flow flow) {
        StateMachineSpec.Builder b = StateMachineSpec.startingAt(flow.initialState());
        for (TradingFlows.Transition t : flow.transitions()) {
            b.on(t.from(), t.eventType(), t.to(), commandClass(t.payloadClass()));
        }
        return b.build();
    }
}
