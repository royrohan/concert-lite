package io.concert.samples;

import io.concert.sdk.StateMachineSpec;

public class PaymentStateMachine extends SampleStateMachine {

    public static final String TYPE = "payment";

    public static final StateMachineSpec SPEC = StateMachineSpec.startingAt("PENDING")
            .on("PENDING", "authorize", "AUTHORIZED")
            .on("PENDING", "decline", "DECLINED")
            .on("AUTHORIZED", "capture", "CAPTURED")
            .on("AUTHORIZED", "void", "VOIDED")
            .on("CAPTURED", "refund", "REFUNDED")
            .build();

    @Override
    protected StateMachineSpec spec() {
        return SPEC;
    }
}
