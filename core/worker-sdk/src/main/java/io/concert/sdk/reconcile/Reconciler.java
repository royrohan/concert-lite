package io.concert.sdk.reconcile;

import io.concert.common.api.SmStateRow;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.sdk.StateMachineSpec;
import io.concert.sink.EntitySnapshot;
import io.concert.store.StateStore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Brings the DuckDB sink back in line with the StateStore for one smType: every entity whose stored
 * state is terminal per the spec must have a DuckDB row with the store's version.
 *
 * <ol>
 *   <li>{@link StateStore#scanStates} with prefix {@code <smType>:} (works on every backend), keep the
 *       terminal ones;
 *   <li>per chunk, ask the sink for the stored versions ({@link SinkVersions}) and classify: missing (no
 *       row), stale (lower version), ahead (higher: reported only);
 *   <li>unless a dry run, for each missing / stale entity <b>re-read</b> it ({@link StateStore#loadState})
 *       and publish a snapshot of that current row through the {@link EntitySnapshotPublisher} (Kafka,
 *       like the workflow's own publish), but only if it is still terminal and not older than what was
 *       scanned. The snapshot topic is compacted (latest record per key wins), so a republish must never
 *       carry an older version than the store's.
 * </ol>
 *
 * Republishing is idempotent (sinks upsert by {@code (entityId, version)}), so a repeated or concurrent
 * run is harmless. The snapshot's {@code createdAt} is unknown from the store ({@code null}); its
 * {@code completedAt} is the projection's last update.
 */
public final class Reconciler {
    private static final Logger log = LoggerFactory.getLogger(Reconciler.class);

    private final StateStore store;
    private final StateMachineSpec spec;
    private final SinkVersions sink;
    private final EntitySnapshotPublisher publisher;

    public Reconciler(StateStore store, StateMachineSpec spec, SinkVersions sink, EntitySnapshotPublisher publisher) {
        this.store = store;
        this.spec = spec;
        this.sink = sink;
        this.publisher = publisher;
    }

    /** @param progress called after every chunk with the counts so far (heartbeats) */
    public ReconcileReport run(ReconcileRequest req, Consumer<ReconcileReport> progress) {
        long t0 = System.currentTimeMillis();
        List<SmStateRow> rows = store.scanStates(req.smType() + ":");
        List<SmStateRow> terminal = rows.stream().filter(r -> r.state() != null && spec.isTerminal(r.state())).toList();
        long missing = 0;
        long stale = 0;
        long ahead = 0;
        long republished = 0;
        long changed = 0;
        for (int from = 0; from < terminal.size(); from += req.chunkSize()) {
            List<SmStateRow> chunk = terminal.subList(from, Math.min(terminal.size(), from + req.chunkSize()));
            Optional<Map<String, Long>> stored = sink.versions(req.smType(), chunk.stream().map(SmStateRow::workflowId).toList());
            if (stored.isEmpty()) {
                return ReconcileReport.skipped(req.smType(), req.dryRun(), "not a root of the DuckDB sink (SINK_ROOTS)");
            }
            for (SmStateRow row : chunk) {
                Long v = stored.get().get(row.workflowId());
                if (v != null && v >= row.version()) {
                    if (v > row.version()) {
                        ahead++;
                    }
                    continue;
                }
                if (v == null) {
                    missing++;
                } else {
                    stale++;
                }
                if (req.dryRun()) {
                    continue;
                }
                SmStateRow current = store.loadState(row.workflowId()).orElse(null);
                if (current == null || current.version() < row.version() || current.state() == null
                        || !spec.isTerminal(current.state())) {
                    changed++;
                    continue;
                }
                publisher.publish(snapshot(current));
                republished++;
            }
            progress.accept(new ReconcileReport(req.smType(), req.dryRun(), rows.size(), terminal.size(), missing, stale,
                    ahead, republished, changed, null, System.currentTimeMillis() - t0));
        }
        ReconcileReport report = new ReconcileReport(req.smType(), req.dryRun(), rows.size(), terminal.size(), missing,
                stale, ahead, republished, changed, null, System.currentTimeMillis() - t0);
        log.info("reconcile {}{}: scanned {}, terminal {}, missing {}, stale {}, ahead {}, republished {}, changed {} ({} ms)",
                req.smType(), req.dryRun() ? " (dry run)" : "", report.scanned(), report.terminal(), missing, stale, ahead,
                republished, changed, report.elapsedMs());
        return report;
    }

    static EntitySnapshot snapshot(SmStateRow row) {
        return new EntitySnapshot(row.workflowId(), row.smType(), row.state(), row.version(), null,
                row.updatedAtMillis() > 0 ? Instant.ofEpochMilli(row.updatedAtMillis()) : null, row.data());
    }
}
