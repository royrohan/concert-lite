package io.concert.samples;

import io.concert.sdk.StateMachineSpec;

public class OrderStateMachine extends SampleStateMachine {

    public static final String TYPE = "order";

    public static final StateMachineSpec SPEC = StateMachineSpec.startingAt("CREATED")
            .on("CREATED", "pay", "PAID")
            .on("CREATED", "cancel", "CANCELLED")
            .on("PAID", "ship", "SHIPPED")
            .on("PAID", "cancel", "REFUND_PENDING")
            .on("SHIPPED", "deliver", "DELIVERED")
            .on("REFUND_PENDING", "refunded", "CANCELLED")
            .build();

    @Override
    protected StateMachineSpec spec() {
        return SPEC;
    }
}
