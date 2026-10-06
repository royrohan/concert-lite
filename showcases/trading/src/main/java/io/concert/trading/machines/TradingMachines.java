package io.concert.trading.machines;

import io.concert.sdk.AbstractStateMachine;
import io.concert.sdk.MachineCatalog;
import io.concert.sdk.StateMachineRegistry;
import io.concert.sdk.StateMachineSpec;
import io.concert.trading.CommonModel;
import io.concert.trading.OrderModel;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The trading machines by smType, with their specs and model diagram (for workers and the trace UI). */
public final class TradingMachines {
    private TradingMachines() {}

    /** @param modelMermaid Mermaid class diagram of the aggregates ({@code order.pure} plus {@code common.pure}) */
    public record Machine(String smType, Class<? extends AbstractStateMachine> impl, StateMachineSpec spec,
            String modelMermaid) {}

    /** Mermaid class diagram shared by the four machines (their aggregates all live in {@code order.pure}). */
    public static final String MODEL_MERMAID = mermaid(OrderModel.MERMAID, CommonModel.MERMAID);

    /** In {@code TradingFlows.all()} order: order, execution, fill, allocation. */
    public static final Map<String, Machine> ALL = index(
            new Machine(TradingOrderMachine.TYPE, TradingOrderMachine.class, TradingOrderMachine.SPEC, MODEL_MERMAID),
            new Machine(TradingExecutionMachine.TYPE, TradingExecutionMachine.class, TradingExecutionMachine.SPEC, MODEL_MERMAID),
            new Machine(TradingFillMachine.TYPE, TradingFillMachine.class, TradingFillMachine.SPEC, MODEL_MERMAID),
            new Machine(TradingAllocationMachine.TYPE, TradingAllocationMachine.class, TradingAllocationMachine.SPEC,
                    MODEL_MERMAID));

    private static Map<String, Machine> index(Machine... machines) {
        Map<String, Machine> m = new LinkedHashMap<>();
        for (Machine x : machines) {
            m.put(x.smType(), x);
        }
        return Collections.unmodifiableMap(m);
    }

    /** Merges generated {@code classDiagram}s (one per model file) into one diagram. */
    static String mermaid(String... diagrams) {
        StringBuilder sb = new StringBuilder("classDiagram\n");
        for (String d : diagrams) {
            sb.append(d.substring(d.indexOf('\n') + 1));
        }
        return sb.toString();
    }

    /** Registers the four specs in {@link StateMachineRegistry} (trace UI diagrams). */
    public static void registerSpecs() {
        ALL.forEach((type, m) -> StateMachineRegistry.register(type, m.spec()));
    }

    /** The trading machines as a {@link MachineCatalog} (ServiceLoader). */
    public static final class Catalog implements MachineCatalog {

        @Override
        public String name() {
            return "trading";
        }

        @Override
        public List<MachineCatalog.Machine> machines() {
            return ALL.values().stream()
                    .map(m -> new MachineCatalog.Machine(m.smType(), m.impl(), m.spec(), m.modelMermaid())).toList();
        }
    }

    public static Machine get(String smType) {
        Machine m = ALL.get(smType);
        if (m == null) {
            throw new IllegalArgumentException("unknown trading smType " + smType + ", expected one of " + ALL.keySet());
        }
        return m;
    }
}
