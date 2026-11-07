package io.concert.store;

import io.concert.common.TraceRow;
import io.concert.common.api.SmStateRow;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Everything the platform persists: dedupe records, shard checkpoints, entity state projections and
 * trace rows. Implementations are discovered with {@link java.util.ServiceLoader} through
 * {@link StateStoreProvider} and selected with {@code STORE_KIND} (postgres, dsql, dynamo, spanner);
 * see {@link Stores}. Every implementation must pass {@code StateStoreContract}.
 */
public interface StateStore extends AutoCloseable {

    /** The STORE_KIND this store serves, e.g. {@code postgres}. */
    String kind();

    /**
     * Dedupe. Records the ids as RECEIVED and returns the ones that must be dispatched: ids never
     * seen before, plus ids that were received but never marked dispatched (a crash between the two
     * steps). Everything else is a duplicate to drop.
     */
    Set<String> claimForDispatch(Collection<String> eventIds, long nowMillis);

    void markDispatched(Collection<String> eventIds);

    /** For tests and the dashboard. */
    Optional<String> processedStatus(String eventId);

    Optional<String> loadCheckpoint(String stream, String shardId);

    void saveCheckpoint(String stream, String shardId, String sequenceNumber);

    /**
     * Upserts the entity projection only if {@code row.version()} is newer than what is stored, so
     * concurrent or out-of-order writes from the entity workflow converge. The trace row (may be
     * null) is written in the same transaction where the backend allows it.
     *
     * <p>{@code row.data()} is a JSON document stored in the backend's native JSON type (Postgres
     * jsonb, DynamoDB map, Spanner JSON; DSQL text). It round-trips as equivalent JSON, not
     * byte-identical text. A value that is not JSON is stored as a JSON string, see {@link JsonData}.
     */
    void upsertState(SmStateRow row, TraceRow trace);

    Optional<SmStateRow> loadState(String workflowId);

    void appendTrace(List<TraceRow> rows);

    List<TraceRow> traceForEvent(String eventId);

    List<TraceRow> traceForWorkflow(String workflowId, int limit);

    /** Latest terminal trace rows (DONE, DROPPED, FAILED, REJECTED), newest first; for the dashboard. */
    List<TraceRow> recentEvents(int limit);

    /**
     * Trace rows of every stage at or after {@code sinceMillis}, newest first, at most {@code limit}; the
     * dashboard's flow metrics aggregate them per flow. The default serves only terminal rows (from
     * {@link #recentEvents}), for stores that index nothing else by time.
     */
    default List<TraceRow> tracesSince(long sinceMillis, int limit) {
        return recentEvents(limit).stream().filter(t -> t.tsMillis() >= sinceMillis).toList();
    }

    /**
     * Deletes a flow's runtime rows (the dashboard's Delete flow): the {@code sm_state} rows of its smTypes (entities,
     * event lifecycle and keyed-state types), the trace rows of workflows whose id starts with one of the prefixes, and
     * the shard checkpoints of its stream. Dedupe rows are not attributable to a flow and expire with their TTL.
     *
     * @return rows deleted
     */
    default long purgeFlow(java.util.Set<String> smTypes, List<String> workflowIdPrefixes, String stream) {
        throw new UnsupportedOperationException(kind() + " store does not support purging a flow");
    }

    /**
     * Deletes dedupe rows older than the cutoff and returns how many. Stores with native expiry
     * (DynamoDB TTL) may return 0 and let the database do it.
     */
    int sweepProcessed(long olderThanMillis, int batchSize);

    // ---- inspection: for verification tools and tests, never on the hot path (may scan) ----

    /** All entity projections whose workflow id starts with the prefix. */
    List<SmStateRow> scanStates(String workflowIdPrefix);

    /**
     * Entity projections under the prefix whose JSON data has {@code value} at {@code jsonPath}
     * (dotted, e.g. {@code customer.name}); compared as text against the JSON scalar. Runs inside
     * the database on its native JSON type.
     */
    List<SmStateRow> scanStatesWhere(String workflowIdPrefix, String jsonPath, String value);

    /** Dedupe status -> count for event ids starting with the prefix. */
    Map<String, Long> processedStatusCounts(String eventIdPrefix);

    /** Trace rows of one stage for event ids starting with the prefix (any order). */
    List<TraceRow> scanTraces(String eventIdPrefix, TraceRow.Stage stage);

    @Override
    void close();
}
