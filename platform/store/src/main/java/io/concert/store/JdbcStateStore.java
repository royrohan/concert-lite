package io.concert.store;

import com.zaxxer.hikari.HikariDataSource;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.common.TraceRow;
import io.concert.common.api.CommitOutcome;
import io.concert.common.api.EntityLoad;
import io.concert.common.api.SmStateRow;
import io.concert.common.api.TransitionCommit;
import io.concert.common.api.TransitionResult;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * Plain JDBC implementation that runs unchanged on Postgres and Aurora DSQL. It sticks to the DSQL
 * subset: multi-row VALUES and IN lists instead of arrays, no FKs, short transactions retried on
 * OCC conflicts.
 */
public final class JdbcStateStore implements StateStore {

    private final DataSource ds;
    private final String kind;

    public JdbcStateStore(DataSource ds) {
        this(ds, "postgres");
    }

    public JdbcStateStore(DataSource ds, String kind) {
        this.ds = ds;
        this.kind = kind;
    }

    @Override
    public String kind() {
        return kind;
    }

    public DataSource dataSource() {
        return ds;
    }

    @Override
    public Set<String> claimForDispatch(Collection<String> eventIds, long nowMillis) {
        List<String> ids = List.copyOf(new LinkedHashSet<>(eventIds));
        if (ids.isEmpty()) {
            return Set.of();
        }
        return OccRetry.run(() -> {
            Set<String> claimed = new HashSet<>();
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(true);
                String insert = "INSERT INTO processed_event (event_id, received_at, status) VALUES "
                        + repeat("(?, ?, 'RECEIVED')", ids.size())
                        + " ON CONFLICT (event_id) DO NOTHING RETURNING event_id";
                try (PreparedStatement ps = c.prepareStatement(insert)) {
                    Timestamp ts = new Timestamp(nowMillis);
                    int i = 1;
                    for (String id : ids) {
                        ps.setString(i++, id);
                        ps.setTimestamp(i++, ts);
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            claimed.add(rs.getString(1));
                        }
                    }
                }
                if (claimed.size() < ids.size()) {
                    // Seen before: re-dispatch only if a previous attempt never got past RECEIVED.
                    List<String> seen = ids.stream().filter(id -> !claimed.contains(id)).toList();
                    String select = "SELECT event_id FROM processed_event WHERE status = 'RECEIVED' AND event_id IN ("
                            + placeholders(seen.size()) + ")";
                    try (PreparedStatement ps = c.prepareStatement(select)) {
                        bindStrings(ps, 1, seen);
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                claimed.add(rs.getString(1));
                            }
                        }
                    }
                }
            }
            return claimed;
        });
    }

    @Override
    public void markDispatched(Collection<String> eventIds) {
        List<String> ids = List.copyOf(new LinkedHashSet<>(eventIds));
        if (ids.isEmpty()) {
            return;
        }
        OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "UPDATE processed_event SET status = 'DISPATCHED' WHERE event_id IN ("
                                    + placeholders(ids.size()) + ")")) {
                bindStrings(ps, 1, ids);
                return ps.executeUpdate();
            }
        });
    }

    @Override
    public Optional<String> processedStatus(String eventId) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement("SELECT status FROM processed_event WHERE event_id = ?")) {
                ps.setString(1, eventId);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? Optional.of(rs.getString(1)) : Optional.<String>empty();
                }
            }
        });
    }

    @Override
    public Optional<String> loadCheckpoint(String stream, String shardId) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "SELECT seq_no FROM shard_checkpoint WHERE stream = ? AND shard_id = ?")) {
                ps.setString(1, stream);
                ps.setString(2, shardId);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? Optional.of(rs.getString(1)) : Optional.<String>empty();
                }
            }
        });
    }

    @Override
    public void saveCheckpoint(String stream, String shardId, String sequenceNumber) {
        OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "INSERT INTO shard_checkpoint (stream, shard_id, seq_no, updated_at) VALUES (?, ?, ?, now()) "
                                    + "ON CONFLICT (stream, shard_id) DO UPDATE SET seq_no = EXCLUDED.seq_no, updated_at = now()")) {
                ps.setString(1, stream);
                ps.setString(2, shardId);
                ps.setString(3, sequenceNumber);
                return ps.executeUpdate();
            }
        });
    }

    @Override
    public void upsertState(SmStateRow row, TraceRow trace) {
        OccRetry.run(() -> {
            try (Connection c = ds.getConnection()) {
                // Only open a transaction when the trace row must commit with the state.
                c.setAutoCommit(trace == null);
                try {
                    try (PreparedStatement ps = c.prepareStatement(
                            "INSERT INTO sm_state (workflow_id, sm_type, state, data, version, last_event_id, updated_at) "
                                    + "VALUES (?, ?, ?, " + dataParam() + ", ?, ?, ?) "
                                    + "ON CONFLICT (workflow_id) DO UPDATE SET state = EXCLUDED.state, data = EXCLUDED.data, "
                                    + "version = EXCLUDED.version, last_event_id = EXCLUDED.last_event_id, "
                                    + "updated_at = EXCLUDED.updated_at WHERE sm_state.version < EXCLUDED.version")) {
                        ps.setString(1, row.workflowId());
                        ps.setString(2, row.smType());
                        ps.setString(3, row.state());
                        ps.setString(4, JsonData.normalize(row.data()));
                        ps.setLong(5, row.version());
                        ps.setString(6, row.lastEventId());
                        ps.setTimestamp(7, new Timestamp(row.updatedAtMillis()));
                        ps.executeUpdate();
                    }
                    if (trace != null) {
                        insertTrace(c, List.of(trace));
                        c.commit();
                    }
                } catch (SQLException e) {
                    if (trace != null) {
                        c.rollback();
                    }
                    throw e;
                } finally {
                    c.setAutoCommit(true);
                }
            }
            return null;
        });
    }

    @Override
    public Optional<SmStateRow> loadState(String workflowId) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "SELECT workflow_id, sm_type, state, data, version, last_event_id, updated_at "
                                    + "FROM sm_state WHERE workflow_id = ?")) {
                ps.setString(1, workflowId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.<SmStateRow>empty();
                    }
                    return Optional.of(new SmStateRow(
                            rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                            rs.getLong(5), rs.getString(6), rs.getTimestamp(7).getTime()));
                }
            }
        });
    }

    // ---------------------------------------------------------------------------------------------- DB-truth

    private static final String STATE_COLUMNS = "workflow_id, sm_type, state, data, version, last_event_id, updated_at";

    private static Optional<SmStateRow> readState(Connection c, String workflowId, boolean forUpdate) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + STATE_COLUMNS + " FROM sm_state WHERE workflow_id = ?"
                + (forUpdate ? " FOR UPDATE" : ""))) {
            ps.setString(1, workflowId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new SmStateRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getLong(5), rs.getString(6), rs.getTimestamp(7).getTime()));
            }
        }
    }

    private static TransitionResult readApplied(Connection c, String workflowId, String eventId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT from_state, to_state, accepted, message, version "
                + "FROM applied_event WHERE workflow_id = ? AND event_id = ?")) {
            ps.setString(1, workflowId);
            ps.setString(2, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? new TransitionResult(eventId, rs.getString(1), rs.getString(2), rs.getBoolean(3),
                        rs.getString(4), rs.getLong(5)) : null;
            }
        }
    }

    @Override
    public EntityLoad loadEntity(String workflowId, String eventId) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection()) {
                return new EntityLoad(readState(c, workflowId, false).orElse(null),
                        eventId == null ? null : readApplied(c, workflowId, eventId));
            }
        });
    }

    @Override
    public CommitOutcome commitTransition(TransitionCommit t) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try {
                    CommitOutcome out = commit(c, t);
                    if (out.status() == CommitOutcome.Status.COMMITTED) {
                        c.commit();
                    } else {
                        c.rollback();
                    }
                    return out;
                } catch (SQLException | RuntimeException e) {
                    c.rollback();
                    throw e;
                } finally {
                    c.setAutoCommit(true);
                }
            }
        });
    }

    /** The transition's statements, in one transaction; anything but COMMITTED is rolled back by the caller. */
    private CommitOutcome commit(Connection c, TransitionCommit t) throws SQLException {
        TransitionResult r = t.result();
        Timestamp now = new Timestamp(t.nowMillis());
        // 1. the applied-event record: its primary key turns a redelivery into a no-op
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO applied_event (workflow_id, event_id, accepted, "
                + "from_state, to_state, version, message, applied_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (workflow_id, event_id) DO NOTHING")) {
            ps.setString(1, t.workflowId());
            ps.setString(2, t.eventId());
            ps.setBoolean(3, r.accepted());
            ps.setString(4, r.fromState());
            ps.setString(5, r.toState());
            ps.setLong(6, r.version());
            ps.setString(7, r.message());
            ps.setTimestamp(8, now);
            if (ps.executeUpdate() == 0) {
                return CommitOutcome.alreadyApplied(readApplied(c, t.workflowId(), t.eventId()));
            }
        }
        if (!r.accepted()) {
            // a rejection was decided on expectedVersion: it stands only if the entity is still there
            long current = readState(c, t.workflowId(), true).map(SmStateRow::version).orElse(0L);
            return current == t.expectedVersion() ? CommitOutcome.committed(r)
                    : CommitOutcome.conflict(readState(c, t.workflowId(), false).orElse(null));
        }
        // 2. the state row, only from the version the decision was made on
        SmStateRow row = t.row();
        int n;
        try (PreparedStatement ps = c.prepareStatement("UPDATE sm_state SET state = ?, data = " + dataParam()
                + ", version = ?, last_event_id = ?, updated_at = ? WHERE workflow_id = ? AND version = ?")) {
            ps.setString(1, row.state());
            ps.setString(2, JsonData.normalize(row.data()));
            ps.setLong(3, row.version());
            ps.setString(4, row.lastEventId());
            ps.setTimestamp(5, new Timestamp(row.updatedAtMillis()));
            ps.setString(6, row.workflowId());
            ps.setLong(7, t.expectedVersion());
            n = ps.executeUpdate();
        }
        if (n == 0 && t.expectedVersion() == 0) {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO sm_state (" + STATE_COLUMNS + ") VALUES (?, ?, ?, "
                    + dataParam() + ", ?, ?, ?) ON CONFLICT (workflow_id) DO NOTHING")) {
                ps.setString(1, row.workflowId());
                ps.setString(2, row.smType());
                ps.setString(3, row.state());
                ps.setString(4, JsonData.normalize(row.data()));
                ps.setLong(5, row.version());
                ps.setString(6, row.lastEventId());
                ps.setTimestamp(7, new Timestamp(row.updatedAtMillis()));
                n = ps.executeUpdate();
            }
        }
        if (n == 0) {
            return CommitOutcome.conflict(readState(c, t.workflowId(), false).orElse(null));
        }
        // 3. the outbox: what the transition emitted, delivered after commit
        if (!t.outbox().isEmpty()) {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO outbox (event_id, workflow_id, envelope, created_at) "
                    + "VALUES " + repeat("(?, ?, ?, ?)", t.outbox().size()) + " ON CONFLICT (event_id) DO NOTHING")) {
                int i = 1;
                for (EventEnvelope e : t.outbox()) {
                    ps.setString(i++, e.eventId());
                    ps.setString(i++, t.workflowId());
                    ps.setString(i++, Json.write(e));
                    ps.setTimestamp(i++, now);
                }
                ps.executeUpdate();
            }
        }
        return CommitOutcome.committed(r);
    }

    @Override
    public List<EventEnvelope> pendingOutbox(String workflowIdPrefix, long createdBeforeMillis, int limit) {
        String like = likePrefix(workflowIdPrefix);
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement("SELECT envelope FROM outbox WHERE sent_at IS NULL "
                            + "AND created_at < ? AND workflow_id LIKE ? ORDER BY created_at LIMIT ?")) {
                ps.setTimestamp(1, new Timestamp(createdBeforeMillis));
                ps.setString(2, like);
                ps.setInt(3, limit);
                List<EventEnvelope> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(Json.read(rs.getString(1), EventEnvelope.class));
                    }
                }
                return out;
            }
        });
    }

    @Override
    public void markOutboxSent(Collection<String> eventIds) {
        List<String> ids = List.copyOf(new LinkedHashSet<>(eventIds));
        for (int from = 0; from < ids.size(); from += 1000) {
            List<String> batch = ids.subList(from, Math.min(ids.size(), from + 1000));
            OccRetry.run(() -> {
                try (Connection c = ds.getConnection();
                        PreparedStatement ps = c.prepareStatement("UPDATE outbox SET sent_at = now() WHERE sent_at IS NULL "
                                + "AND event_id IN (" + placeholders(batch.size()) + ")")) {
                    bindStrings(ps, 1, batch);
                    return ps.executeUpdate();
                }
            });
        }
    }

    @Override
    public int sweepEntityLog(long outboxSentBeforeMillis, long appliedBeforeMillis, int batchSize) {
        int limit = Math.min(batchSize, 2500); // DSQL: at most 3000 modified rows per transaction
        int total = 0;
        for (String sql : List.of(
                "DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM outbox WHERE sent_at < ? LIMIT ?)",
                "DELETE FROM applied_event WHERE (workflow_id, event_id) IN "
                        + "(SELECT workflow_id, event_id FROM applied_event WHERE applied_at < ? LIMIT ?)")) {
            long before = sql.startsWith("DELETE FROM outbox") ? outboxSentBeforeMillis : appliedBeforeMillis;
            while (true) {
                int n = OccRetry.run(() -> {
                    try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                        ps.setTimestamp(1, new Timestamp(before));
                        ps.setInt(2, limit);
                        return ps.executeUpdate();
                    }
                });
                total += n;
                if (n == 0) {
                    break;
                }
            }
        }
        return total;
    }

    @Override
    public void appendTrace(List<TraceRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        // DSQL caps a transaction at 3000 modified rows.
        for (int from = 0; from < rows.size(); from += 1000) {
            List<TraceRow> chunk = rows.subList(from, Math.min(rows.size(), from + 1000));
            OccRetry.run(() -> {
                try (Connection c = ds.getConnection()) {
                    c.setAutoCommit(true);
                    insertTrace(c, chunk);
                }
                return null;
            });
        }
    }

    private static void insertTrace(Connection c, List<TraceRow> rows) throws SQLException {
        String sql = "INSERT INTO event_trace (trace_id, event_id, ts, stage, workflow_id, lock_key, detail) VALUES "
                + repeat("(?, ?, ?, ?, ?, ?, ?)", rows.size());
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            int i = 1;
            for (TraceRow r : rows) {
                ps.setString(i++, UUID.randomUUID().toString());
                ps.setString(i++, r.eventId());
                ps.setTimestamp(i++, new Timestamp(r.tsMillis()));
                ps.setString(i++, r.stage().name());
                ps.setString(i++, r.workflowId());
                ps.setString(i++, r.lockKey());
                ps.setString(i++, r.detail());
            }
            ps.executeUpdate();
        }
    }

    @Override
    public List<TraceRow> traceForEvent(String eventId) {
        return queryTrace("WHERE event_id = ? ORDER BY ts, stage", eventId, 1000);
    }

    @Override
    public List<TraceRow> traceForWorkflow(String workflowId, int limit) {
        return queryTrace("WHERE workflow_id = ? ORDER BY ts DESC", workflowId, limit);
    }

    private List<TraceRow> queryTrace(String where, String arg, int limit) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "SELECT event_id, ts, stage, workflow_id, lock_key, detail FROM event_trace "
                                    + where + " LIMIT " + limit)) {
                ps.setString(1, arg);
                List<TraceRow> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new TraceRow(
                                rs.getString(1), rs.getTimestamp(2).getTime(), TraceRow.Stage.valueOf(rs.getString(3)),
                                rs.getString(4), rs.getString(5), rs.getString(6)));
                    }
                }
                return out;
            }
        });
    }

    @Override
    public List<TraceRow> recentEvents(int limit) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "SELECT event_id, ts, stage, workflow_id, lock_key, detail FROM event_trace "
                                    + "WHERE stage IN ('DONE', 'DROPPED', 'FAILED', 'REJECTED') ORDER BY ts DESC LIMIT ?")) {
                ps.setInt(1, limit);
                List<TraceRow> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new TraceRow(rs.getString(1), rs.getTimestamp(2).getTime(),
                                TraceRow.Stage.valueOf(rs.getString(3)), rs.getString(4), rs.getString(5), rs.getString(6)));
                    }
                }
                return out;
            }
        });
    }

    @Override
    public List<TraceRow> tracesSince(long sinceMillis, int limit) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "SELECT event_id, ts, stage, workflow_id, lock_key, detail FROM event_trace "
                                    + "WHERE ts >= ? ORDER BY ts DESC LIMIT ?")) {
                ps.setTimestamp(1, new java.sql.Timestamp(sinceMillis));
                ps.setInt(2, limit);
                List<TraceRow> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new TraceRow(rs.getString(1), rs.getTimestamp(2).getTime(),
                                TraceRow.Stage.valueOf(rs.getString(3)), rs.getString(4), rs.getString(5), rs.getString(6)));
                    }
                }
                return out;
            }
        });
    }

    @Override
    public int sweepProcessed(long olderThanMillis, int batchSize) {
        int total = 0;
        while (true) {
            int n = OccRetry.run(() -> {
                try (Connection c = ds.getConnection();
                        PreparedStatement ps = c.prepareStatement(
                                "DELETE FROM processed_event WHERE event_id IN "
                                        + "(SELECT event_id FROM processed_event WHERE received_at < ? LIMIT ?)")) {
                    ps.setTimestamp(1, new Timestamp(olderThanMillis));
                    ps.setInt(2, Math.min(batchSize, 2500));
                    return ps.executeUpdate();
                }
            });
            total += n;
            if (n == 0) {
                return total;
            }
        }
    }

    @Override
    public List<SmStateRow> scanStates(String workflowIdPrefix) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "SELECT workflow_id, sm_type, state, data, version, last_event_id, updated_at "
                                    + "FROM sm_state WHERE workflow_id LIKE ?")) {
                ps.setString(1, likePrefix(workflowIdPrefix));
                List<SmStateRow> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new SmStateRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                                rs.getLong(5), rs.getString(6), rs.getTimestamp(7).getTime()));
                    }
                }
                return out;
            }
        });
    }

    @Override
    public java.util.Map<String, Long> processedStatusCounts(String eventIdPrefix) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "SELECT status, count(*) FROM processed_event WHERE event_id LIKE ? GROUP BY status")) {
                ps.setString(1, likePrefix(eventIdPrefix));
                java.util.Map<String, Long> out = new java.util.TreeMap<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.put(rs.getString(1), rs.getLong(2));
                    }
                }
                return out;
            }
        });
    }

    @Override
    public List<TraceRow> scanTraces(String eventIdPrefix, TraceRow.Stage stage) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "SELECT event_id, ts, stage, workflow_id, lock_key, detail FROM event_trace "
                                    + "WHERE stage = ? AND event_id LIKE ?")) {
                ps.setString(1, stage.name());
                ps.setString(2, likePrefix(eventIdPrefix));
                List<TraceRow> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new TraceRow(rs.getString(1), rs.getTimestamp(2).getTime(),
                                TraceRow.Stage.valueOf(rs.getString(3)), rs.getString(4), rs.getString(5), rs.getString(6)));
                    }
                }
                return out;
            }
        });
    }

    @Override
    public List<SmStateRow> scanStatesWhere(String workflowIdPrefix, String jsonPath, String value) {
        return OccRetry.run(() -> {
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "SELECT workflow_id, sm_type, state, data, version, last_event_id, updated_at FROM sm_state "
                                    + "WHERE workflow_id LIKE ? AND " + dataExpr() + " #>> ?::text[] = ?")) {
                ps.setString(1, likePrefix(workflowIdPrefix));
                ps.setString(2, "{" + String.join(",", jsonPath.split("\\.")) + "}");
                ps.setString(3, value);
                List<SmStateRow> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new SmStateRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                                rs.getLong(5), rs.getString(6), rs.getTimestamp(7).getTime()));
                    }
                }
                return out;
            }
        });
    }

    /** Postgres stores data as jsonb; DSQL keeps text and casts when querying. */
    private boolean jsonb() {
        return !kind.equals("dsql");
    }

    private String dataParam() {
        return jsonb() ? "?::jsonb" : "?";
    }

    private String dataExpr() {
        return jsonb() ? "data" : "data::json";
    }

    @Override
    public long purgeFlow(Set<String> smTypes, List<String> workflowIdPrefixes, String stream) {
        long total = 0;
        // batched by primary key: DSQL caps the rows one transaction may touch
        for (String t : smTypes) {
            total += deleteBatched("DELETE FROM sm_state WHERE workflow_id IN "
                    + "(SELECT workflow_id FROM sm_state WHERE sm_type = ? LIMIT 2500)", t);
        }
        for (String p : workflowIdPrefixes) {
            total += deleteBatched("DELETE FROM event_trace WHERE trace_id IN "
                    + "(SELECT trace_id FROM event_trace WHERE workflow_id LIKE ? LIMIT 2500)", likePrefix(p));
        }
        total += deleteBatched("DELETE FROM shard_checkpoint WHERE stream = ?", stream);
        return total;
    }

    private long deleteBatched(String sql, String arg) {
        long total = 0;
        while (true) {
            int n = OccRetry.run(() -> {
                try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setString(1, arg);
                    return ps.executeUpdate();
                }
            });
            total += n;
            if (n == 0 || !sql.contains("LIMIT")) {
                return total;
            }
        }
    }

    private static String likePrefix(String prefix) {
        return prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%";
    }

    @Override
    public void close() {
        if (ds instanceof HikariDataSource h) {
            h.close();
        }
    }

    private static String repeat(String tuple, int n) {
        return String.join(", ", java.util.Collections.nCopies(n, tuple));
    }

    private static String placeholders(int n) {
        return repeat("?", n);
    }

    private static void bindStrings(PreparedStatement ps, int start, List<String> values) throws SQLException {
        int i = start;
        for (String v : values) {
            ps.setString(i++, v);
        }
    }
}
