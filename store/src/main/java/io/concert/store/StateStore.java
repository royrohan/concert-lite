package io.concert.store;

import io.concert.common.TraceRow;
import io.concert.common.api.SmStateRow;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Everything the platform persists in Aurora DSQL (or the local Postgres stand-in). */
public interface StateStore extends AutoCloseable {

    /**
     * Dedupe. Records the ids as RECEIVED and returns the ones that must be dispatched: ids never
     * seen before, plus ids that were received but never marked dispatched (a crash between the two
     * steps). Everything else is a duplicate to drop.
     */
    Set<String> claimForDispatch(Collection<String> eventIds, long nowMillis);

    void markDispatched(Collection<String> eventIds);

    /** For tests and the trace UI. */
    Optional<String> processedStatus(String eventId);

    Optional<String> loadCheckpoint(String stream, String shardId);

    void saveCheckpoint(String stream, String shardId, String sequenceNumber);

    /**
     * Upserts the entity projection only if {@code row.version()} is newer than what is stored, so
     * concurrent or out-of-order writes from the entity workflow converge. The trace row (may be
     * null) is written in the same transaction.
     */
    void upsertState(SmStateRow row, TraceRow trace);

    Optional<SmStateRow> loadState(String workflowId);

    void appendTrace(List<TraceRow> rows);

    List<TraceRow> traceForEvent(String eventId);

    List<TraceRow> traceForWorkflow(String workflowId, int limit);

    /** Deletes dedupe rows older than the cutoff, in batches below DSQL's 3000-rows-per-tx limit. */
    int sweepProcessed(long olderThanMillis, int batchSize);

    @Override
    void close();
}
