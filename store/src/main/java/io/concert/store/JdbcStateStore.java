package io.concert.store;

import com.zaxxer.hikari.HikariDataSource;
import io.concert.common.TraceRow;
import io.concert.common.api.SmStateRow;
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

    public JdbcStateStore(DataSource ds) {
        this.ds = ds;
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
                                    + "VALUES (?, ?, ?, ?, ?, ?, ?) "
                                    + "ON CONFLICT (workflow_id) DO UPDATE SET state = EXCLUDED.state, data = EXCLUDED.data, "
                                    + "version = EXCLUDED.version, last_event_id = EXCLUDED.last_event_id, "
                                    + "updated_at = EXCLUDED.updated_at WHERE sm_state.version < EXCLUDED.version")) {
                        ps.setString(1, row.workflowId());
                        ps.setString(2, row.smType());
                        ps.setString(3, row.state());
                        ps.setString(4, row.data());
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

    /** Most recent events by their last trace row, for the trace UI's landing list. */
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
