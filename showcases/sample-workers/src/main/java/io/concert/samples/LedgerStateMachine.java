package io.concert.samples;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.sdk.StateMachineSpec;

/**
 * Self-looping machine used by ordering tests and benchmarks: every {@code append} event carries
 * {@code {"seq": n}}; the data tracks how many arrived and how many arrived out of order.
 */
public class LedgerStateMachine extends SampleStateMachine {

    public static final String TYPE = "ledger";

    public static final StateMachineSpec SPEC = StateMachineSpec.startingAt("OPEN")
            .on("OPEN", "append", "OPEN")
            .on("OPEN", "close", "CLOSED")
            .build();

    public record Ledger(long count, long lastSeq, long outOfOrder) {}

    @Override
    protected StateMachineSpec spec() {
        return SPEC;
    }

    @Override
    protected String applyData(String fromState, String toState, String currentData, EventEnvelope event) {
        Ledger cur = currentData == null ? new Ledger(0, -1, 0) : Json.read(currentData, Ledger.class);
        if (event.payload() == null) {
            return Json.write(cur);
        }
        JsonNode p = Json.read(event.payload(), JsonNode.class);
        long seq = p.path("seq").asLong(-1);
        long ooo = cur.outOfOrder() + (seq >= 0 && seq < cur.lastSeq() ? 1 : 0);
        return Json.write(new Ledger(cur.count() + 1, Math.max(seq, cur.lastSeq()), ooo));
    }
}
