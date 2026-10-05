package io.concert.samples;

import io.concert.sdk.AbstractStateMachine;
import io.concert.sdk.StateMachineRegistry;
import io.concert.sdk.StateMachineSpec;
import java.util.Map;

public final class SampleMachines {
    private SampleMachines() {}

    public record Machine(Class<? extends AbstractStateMachine> impl, StateMachineSpec spec) {}

    public static final Map<String, Machine> ALL = Map.of(
            OrderStateMachine.TYPE, new Machine(OrderStateMachine.class, OrderStateMachine.SPEC),
            PaymentStateMachine.TYPE, new Machine(PaymentStateMachine.class, PaymentStateMachine.SPEC),
            ShipmentStateMachine.TYPE, new Machine(ShipmentStateMachine.class, ShipmentStateMachine.SPEC),
            LedgerStateMachine.TYPE, new Machine(LedgerStateMachine.class, LedgerStateMachine.SPEC));

    public static void registerSpecs() {
        ALL.forEach((type, m) -> StateMachineRegistry.register(type, m.spec()));
    }

    public static Machine get(String type) {
        Machine m = ALL.get(type);
        if (m == null) {
            throw new IllegalArgumentException("unknown SM_TYPE " + type + ", expected one of " + ALL.keySet());
        }
        return m;
    }
}
