package io.concert.sdk.reconcile;

/**
 * Result of a reconciliation run for one smType.
 *
 * @param scanned entity projections read from the StateStore for the smType
 * @param terminal those in a terminal state (the ones that must be in the sinks)
 * @param missing terminal entities without a row in DuckDB
 * @param stale terminal entities whose DuckDB row has a lower version than the store
 * @param ahead DuckDB rows with a <i>higher</i> version than the store (store reset?): reported, not touched
 * @param republished current snapshots published again (0 in a dry run)
 * @param changed missing/stale entities that changed between the scan and the publish and were skipped
 *     (they left the terminal state; a new terminal transition publishes by itself)
 * @param skipped why the smType was not reconciled at all ({@code null} if it was)
 */
public record ReconcileReport(
        String smType, boolean dryRun, long scanned, long terminal, long missing, long stale, long ahead,
        long republished, long changed, String skipped, long elapsedMs) {

    public static ReconcileReport skipped(String smType, boolean dryRun, String why) {
        return new ReconcileReport(smType, dryRun, 0, 0, 0, 0, 0, 0, 0, why, 0);
    }
}
