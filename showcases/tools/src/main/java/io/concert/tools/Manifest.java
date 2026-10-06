package io.concert.tools;

/**
 * What the load generator sent, so the verifier knows the exact expected end state.
 *
 * @param perKey events sent to ledger {@code <runId>-K<i>}
 * @param accounts multi-key ledgers (odd i) also lock {@code account:<runId>-A<(i/2) % accounts>}
 * @param style {@code entity} (ledger state machines) or {@code event} (DemoEvents {@code Tick}s, keyed state
 *     {@code state:evc:<runId>-K<i>})
 * @param chainedPerKey event style: Ticks sent with {@code chain = true} per key (each emits one ChainTick)
 */
public record Manifest(String runId, int keys, int accounts, long total, long[] perKey, long startedAt, long finishedAt,
        String style, long[] chainedPerKey) {

    public Manifest {
        style = style == null ? "entity" : style;
        chainedPerKey = chainedPerKey == null ? new long[0] : chainedPerKey;
    }

    boolean eventStyle() {
        return "event".equals(style);
    }
}
