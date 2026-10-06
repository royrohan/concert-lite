package io.concert.store.spanner;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import io.concert.common.TraceRow;
import io.concert.common.api.SmStateRow;
import com.google.cloud.spanner.Value;
import io.concert.store.JsonData;
import io.concert.store.StateStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Cloud Spanner backend (GoogleSQL, native client):
 *
 * <ul>
 *   <li>dedupe claim: one read-write transaction per batch, reading the batch's keys and inserting
 *       the missing ones; Spanner retries the transaction on contention,
 *   <li>state upsert: read-check-write in a transaction, so the state and its trace row commit
 *       atomically and stay monotonic by version,
 *   <li>idempotent writes (dispatched marks, checkpoints, traces) use {@code writeAtLeastOnce}: one
 *       round trip, no replay protection needed,
 *   <li>sweep: partitioned DML.
 * </ul>
 *
 * Note for real Spanner: event ids that increase monotonically hotspot one split; prefer
 * high-entropy ids (UUIDs, hashes) in production.
 */
public final class SpannerStateStore implements StateStore {

    private static final List<String> TERMINAL = List.of("DONE", "DROPPED", "FAILED", "REJECTED");
    private static final String TRACE_COLS = "event_id, ts, stage, workflow_id, lock_key, detail";
    /** data_json (native JSON) supersedes the original STRING column, which only old rows still use. */
    private static final String STATE_COLS =
            "workflow_id, sm_type, state, COALESCE(TO_JSON_STRING(data_json), data), version, last_event_id, updated_at";

    private final Spanner spanner;
    private final DatabaseClient db;

    public SpannerStateStore(Spanner spanner, DatabaseId id) {
        this.spanner = spanner;
        this.db = spanner.getDatabaseClient(id);
    }

    @Override
    public String kind() {
        return "spanner";
    }

    // ---------------------------------------------------------------- dedupe

    @Override
    public Set<String> claimForDispatch(Collection<String> eventIds, long nowMillis) {
        List<String> ids = List.copyOf(new LinkedHashSet<>(eventIds));
        if (ids.isEmpty()) {
            return Set.of();
        }
        return db.readWriteTransaction().run(tx -> {
            KeySet.Builder keys = KeySet.newBuilder();
            ids.forEach(id -> keys.addKey(Key.of(id)));
            Map<String, String> existing = new HashMap<>();
            try (ResultSet rs = tx.read("processed_event", keys.build(), List.of("event_id", "status"))) {
                while (rs.next()) {
                    existing.put(rs.getString(0), rs.getString(1));
                }
            }
            Set<String> claimed = new HashSet<>();
            List<Mutation> inserts = new ArrayList<>();
            for (String id : ids) {
                String status = existing.get(id);
                if (status == null) {
                    inserts.add(Mutation.newInsertBuilder("processed_event")
                            .set("event_id").to(id).set("received_at").to(ts(nowMillis)).set("status").to("RECEIVED").build());
                    claimed.add(id);
                } else if (status.equals("RECEIVED")) {
                    claimed.add(id); // never got past RECEIVED: re-dispatch
                }
            }
            tx.buffer(inserts);
            return claimed;
        });
    }

    @Override
    public void markDispatched(Collection<String> eventIds) {
        List<Mutation> m = new LinkedHashSet<>(eventIds).stream()
                .map(id -> Mutation.newUpdateBuilder("processed_event").set("event_id").to(id).set("status").to("DISPATCHED").build())
                .toList();
        if (!m.isEmpty()) {
            db.writeAtLeastOnce(m);
        }
    }

    @Override
    public Optional<String> processedStatus(String eventId) {
        Struct row = db.singleUse().readRow("processed_event", Key.of(eventId), List.of("status"));
        return row == null ? Optional.empty() : Optional.of(row.getString(0));
    }

    @Override
    public int sweepProcessed(long olderThanMillis, int batchSize) {
        return (int) db.executePartitionedUpdate(Statement.newBuilder(
                "DELETE FROM processed_event WHERE received_at < @t").bind("t").to(ts(olderThanMillis)).build());
    }

    @Override
    public Map<String, Long> processedStatusCounts(String eventIdPrefix) {
        Map<String, Long> out = new TreeMap<>();
        query(Statement.newBuilder("SELECT status, COUNT(*) FROM processed_event WHERE STARTS_WITH(event_id, @p) GROUP BY status")
                .bind("p").to(eventIdPrefix).build(), r -> out.put(r.getString(0), r.getLong(1)));
        return out;
    }

    // ---------------------------------------------------------------- checkpoints

    @Override
    public Optional<String> loadCheckpoint(String stream, String shardId) {
        Struct row = db.singleUse().readRow("shard_checkpoint", Key.of(stream, shardId), List.of("seq_no"));
        return row == null ? Optional.empty() : Optional.of(row.getString(0));
    }

    @Override
    public void saveCheckpoint(String stream, String shardId, String sequenceNumber) {
        db.writeAtLeastOnce(List.of(Mutation.newInsertOrUpdateBuilder("shard_checkpoint")
                .set("stream").to(stream).set("shard_id").to(shardId).set("seq_no").to(sequenceNumber)
                .set("updated_at").to(Timestamp.now()).build()));
    }

    // ---------------------------------------------------------------- entity state

    @Override
    public void upsertState(SmStateRow row, TraceRow trace) {
        db.readWriteTransaction().run(tx -> {
            Struct current = tx.readRow("sm_state", Key.of(row.workflowId()), List.of("version"));
            if (current == null || current.getLong(0) < row.version()) {
                tx.buffer(Mutation.newInsertOrUpdateBuilder("sm_state")
                        .set("workflow_id").to(row.workflowId())
                        .set("sm_type").to(row.smType())
                        .set("state").to(row.state())
                        .set("data").to((String) null)
                        .set("data_json").to(Value.json(JsonData.normalize(row.data())))
                        .set("version").to(row.version())
                        .set("last_event_id").to(row.lastEventId())
                        .set("updated_at").to(ts(row.updatedAtMillis()))
                        .build());
            }
            if (trace != null) {
                tx.buffer(traceMutation(trace));
            }
            return null;
        });
    }

    @Override
    public Optional<SmStateRow> loadState(String workflowId) {
        List<SmStateRow> rows = new ArrayList<>();
        query(Statement.newBuilder("SELECT " + STATE_COLS + " FROM sm_state WHERE workflow_id = @w")
                .bind("w").to(workflowId).build(), r -> rows.add(stateRow(r)));
        return rows.stream().findFirst();
    }

    @Override
    public List<SmStateRow> scanStates(String workflowIdPrefix) {
        List<SmStateRow> rows = new ArrayList<>();
        query(Statement.newBuilder("SELECT " + STATE_COLS + " FROM sm_state WHERE STARTS_WITH(workflow_id, @p)")
                .bind("p").to(workflowIdPrefix).build(), r -> rows.add(stateRow(r)));
        return rows;
    }

    @Override
    public List<SmStateRow> scanStatesWhere(String workflowIdPrefix, String jsonPath, String value) {
        List<SmStateRow> rows = new ArrayList<>();
        query(Statement.newBuilder("SELECT " + STATE_COLS + " FROM sm_state "
                        + "WHERE STARTS_WITH(workflow_id, @p) AND JSON_VALUE(data_json, @path) = @v")
                .bind("p").to(workflowIdPrefix).bind("path").to("$." + jsonPath).bind("v").to(value).build(),
                r -> rows.add(stateRow(r)));
        return rows;
    }

    private static SmStateRow stateRow(ResultSet r) {
        // A JSON column holds JSON null distinctly from SQL NULL; for entity data both mean "no data".
        String data = r.isNull(3) || r.getString(3).equals("null") ? null : r.getString(3);
        return new SmStateRow(r.getString(0), r.getString(1), r.getString(2), data,
                r.getLong(4), r.isNull(5) ? null : r.getString(5), millis(r.getTimestamp(6)));
    }

    // ---------------------------------------------------------------- traces

    @Override
    public void appendTrace(List<TraceRow> rows) {
        // Well under Spanner's 80k-mutated-cells-per-commit limit (7 cells per row).
        for (int from = 0; from < rows.size(); from += 1000) {
            db.writeAtLeastOnce(rows.subList(from, Math.min(rows.size(), from + 1000)).stream()
                    .map(SpannerStateStore::traceMutation).toList());
        }
    }

    private static Mutation traceMutation(TraceRow t) {
        return Mutation.newInsertBuilder("event_trace")
                .set("event_id").to(t.eventId())
                .set("trace_id").to(UUID.randomUUID().toString())
                .set("ts").to(ts(t.tsMillis()))
                .set("stage").to(t.stage().name())
                .set("workflow_id").to(t.workflowId())
                .set("lock_key").to(t.lockKey())
                .set("detail").to(t.detail())
                .build();
    }

    @Override
    public List<TraceRow> traceForEvent(String eventId) {
        return traces(Statement.newBuilder("SELECT " + TRACE_COLS + " FROM event_trace WHERE event_id = @e ORDER BY ts, stage LIMIT 1000")
                .bind("e").to(eventId).build());
    }

    @Override
    public List<TraceRow> traceForWorkflow(String workflowId, int limit) {
        return traces(Statement.newBuilder("SELECT " + TRACE_COLS + " FROM event_trace WHERE workflow_id = @w ORDER BY ts DESC LIMIT @l")
                .bind("w").to(workflowId).bind("l").to(limit).build());
    }

    @Override
    public List<TraceRow> recentEvents(int limit) {
        return traces(Statement.newBuilder("SELECT " + TRACE_COLS + " FROM event_trace WHERE stage IN UNNEST(@s) ORDER BY ts DESC LIMIT @l")
                .bind("s").toStringArray(TERMINAL).bind("l").to(limit).build());
    }

    @Override
    public List<TraceRow> scanTraces(String eventIdPrefix, TraceRow.Stage stage) {
        return traces(Statement.newBuilder("SELECT " + TRACE_COLS + " FROM event_trace WHERE stage = @s AND STARTS_WITH(event_id, @p)")
                .bind("s").to(stage.name()).bind("p").to(eventIdPrefix).build());
    }

    private List<TraceRow> traces(Statement st) {
        List<TraceRow> out = new ArrayList<>();
        query(st, r -> out.add(new TraceRow(r.getString(0), millis(r.getTimestamp(1)), TraceRow.Stage.valueOf(r.getString(2)),
                r.isNull(3) ? null : r.getString(3), r.isNull(4) ? null : r.getString(4), r.isNull(5) ? null : r.getString(5))));
        return out;
    }

    // ---------------------------------------------------------------- plumbing

    private void query(Statement st, java.util.function.Consumer<ResultSet> each) {
        try (ResultSet rs = db.singleUse().executeQuery(st)) {
            while (rs.next()) {
                each.accept(rs);
            }
        }
    }

    private static Timestamp ts(long millis) {
        return Timestamp.ofTimeMicroseconds(millis * 1000);
    }

    private static long millis(Timestamp t) {
        return t.getSeconds() * 1000 + t.getNanos() / 1_000_000;
    }

    @Override
    public void close() {
        spanner.close();
    }
}
