package io.concert.tools;

/**
 * What the load generator sent, so the verifier knows the exact expected end state.
 *
 * @param perKey events sent to ledger {@code <runId>-K<i>}
 * @param accounts multi-key ledgers (odd i) also lock {@code account:<runId>-A<(i/2) % accounts>}
 */
public record Manifest(String runId, int keys, int accounts, long total, long[] perKey, long startedAt, long finishedAt) {}
