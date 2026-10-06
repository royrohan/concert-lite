package io.concert.sdk.reconcile;

/**
 * One reconciliation run for one smType.
 *
 * @param dryRun only compare and count, publish nothing
 * @param chunkSize entities compared with the sink per request (and per heartbeat)
 */
public record ReconcileRequest(String smType, boolean dryRun, int chunkSize) {

    public static final int DEFAULT_CHUNK = 500;

    public ReconcileRequest {
        if (chunkSize <= 0) {
            chunkSize = DEFAULT_CHUNK;
        }
    }
}
