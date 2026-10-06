package io.concert.samples;

import io.concert.samples.model.demo.CommonModel;
import io.concert.samples.model.demo.OrderModel;
import io.concert.samples.model.demo.PaymentModel;
import io.concert.samples.model.demo.ShipmentModel;
import io.concert.sdk.AbstractStateMachine;
import io.concert.sdk.MachineCatalog;
import io.concert.sdk.StateMachineRegistry;
import io.concert.sdk.StateMachineSpec;
import java.util.List;
import java.util.Map;

public final class SampleMachines {
    private SampleMachines() {}

    /**
     * @param modelMermaid Mermaid class diagram of the machine's Pure model (its own file plus
     *     {@code common.pure}), or {@code null} for untyped machines
     */
    public record Machine(Class<? extends AbstractStateMachine> impl, StateMachineSpec spec, String modelMermaid) {}

    public static final Map<String, Machine> ALL = Map.of(
            OrderStateMachine.TYPE, new Machine(OrderStateMachine.class, OrderStateMachine.SPEC,
                    mermaid(OrderModel.MERMAID, CommonModel.MERMAID)),
            PaymentStateMachine.TYPE, new Machine(PaymentStateMachine.class, PaymentStateMachine.SPEC,
                    mermaid(PaymentModel.MERMAID, CommonModel.MERMAID)),
            ShipmentStateMachine.TYPE, new Machine(ShipmentStateMachine.class, ShipmentStateMachine.SPEC,
                    mermaid(ShipmentModel.MERMAID, CommonModel.MERMAID)),
            LedgerStateMachine.TYPE, new Machine(LedgerStateMachine.class, LedgerStateMachine.SPEC, null));

    /** Merges generated {@code classDiagram}s (one per model file) into one diagram. */
    static String mermaid(String... diagrams) {
        StringBuilder sb = new StringBuilder("classDiagram\n");
        for (String d : diagrams) {
            sb.append(d.substring(d.indexOf('\n') + 1));
        }
        return sb.toString();
    }

    public static void registerSpecs() {
        ALL.forEach((type, m) -> StateMachineRegistry.register(type, m.spec()));
    }

    /** The sample machines as a {@link MachineCatalog} (ServiceLoader), in the order order, payment, shipment, ledger. */
    public static final class Catalog implements MachineCatalog {

        @Override
        public String name() {
            return "samples";
        }

        @Override
        public List<MachineCatalog.Machine> machines() {
            return List.of(OrderStateMachine.TYPE, PaymentStateMachine.TYPE, ShipmentStateMachine.TYPE, LedgerStateMachine.TYPE)
                    .stream().map(t -> new MachineCatalog.Machine(t, ALL.get(t).impl(), ALL.get(t).spec(), ALL.get(t).modelMermaid()))
                    .toList();
        }
    }

    public static Machine get(String type) {
        Machine m = ALL.get(type);
        if (m == null) {
            throw new IllegalArgumentException("unknown SM_TYPE " + type + ", expected one of " + ALL.keySet());
        }
        return m;
    }
}
