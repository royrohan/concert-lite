package io.concert.samples;

import io.concert.sdk.StateMachineSpec;

public class ShipmentStateMachine extends SampleStateMachine {

    public static final String TYPE = "shipment";

    public static final StateMachineSpec SPEC = StateMachineSpec.startingAt("CREATED")
            .on("CREATED", "pick", "PICKED")
            .on("PICKED", "dispatch", "IN_TRANSIT")
            .on("IN_TRANSIT", "deliver", "DELIVERED")
            .on("IN_TRANSIT", "lose", "LOST")
            .build();

    @Override
    protected StateMachineSpec spec() {
        return SPEC;
    }
}
