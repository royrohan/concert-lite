package io.concert.samples;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.sdk.AbstractStateMachine;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/** Base for the sample machines: a payload field {@code workMs} triggers simulated side-effect work. */
public abstract class SampleStateMachine extends AbstractStateMachine {

    private final SimulatedWork work = Workflow.newLocalActivityStub(SimulatedWork.class,
            LocalActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(30)).build());

    @Override
    protected void onTransition(String fromState, String toState, EventEnvelope event) {
        if (event.payload() == null) {
            return;
        }
        long ms = Json.read(event.payload(), JsonNode.class).path("workMs").asLong(0);
        if (ms > 0) {
            work.work(ms);
        }
    }
}
