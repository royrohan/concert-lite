package io.concert.dashboard;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The Performance page's history: every run, a metrics sample every few seconds, and its timeline, in a DuckDB file
 * ({@code PERF_DB}; in compose {@code /data/perf-runs.duckdb} on the {@code dashboard-data} volume). Kept apart from the
 * DuckDB sink's file: a run may stop the sink, and DuckDB allows one writer per file. Query it with the DuckDB CLI:
 *
 * <pre>
 * perf_runs     (run_id, flow, coordinator_impl, machine, preset, coordinators, workers, rate, duration_sec, lock_mode,
 *                chaos, chaos_every_sec, chaos_kafka, tuning, started_at, ended_at, status)
 * perf_samples  (run_id, ts, t_sec, loading, published_per_sec, dispatched_per_sec, processed_per_sec, completed_per_sec,
 *                completed, errored, rejected, latency_p50, latency_p95, latency_p99, lifetime_p50, lifetime_p95,
 *                lock_waits, instances_up, kafka_up)
 * perf_marks    (run_id, ts, action, target, detail)
 * </pre>
 *
 * Samples cover consecutive windows of {@code interval} seconds, so counts (completed, errored) add up over a run.
 */
final class PerfStore implements AutoCloseable {

    /** One metrics sample of a run. */
    record Sample(long ts, int tSec, boolean loading, Double publishedPerSec, Double dispatchedPerSec, Double processedPerSec,
            Double completedPerSec, Long completed, Long errored, Long rejected, Double latencyP50, Double latencyP95,
            Double latencyP99, Double lifetimeP50, Double lifetimeP95, Long lockWaits, Integer instancesUp, Boolean kafkaUp) {}

    private static final String[] SCHEMA = {
        """
        CREATE TABLE IF NOT EXISTS perf_runs (
            run_id VARCHAR PRIMARY KEY, flow VARCHAR, coordinator_impl VARCHAR, machine VARCHAR, preset VARCHAR,
            coordinators INTEGER, workers INTEGER, rate DOUBLE, duration_sec INTEGER, lock_mode VARCHAR,
            chaos BOOLEAN, chaos_every_sec INTEGER, chaos_kafka BOOLEAN, tuning VARCHAR,
            started_at TIMESTAMP, ended_at TIMESTAMP, status VARCHAR)""",
        """
        CREATE TABLE IF NOT EXISTS perf_samples (
            run_id VARCHAR, ts TIMESTAMP, t_sec INTEGER, loading BOOLEAN,
            published_per_sec DOUBLE, dispatched_per_sec DOUBLE, processed_per_sec DOUBLE, completed_per_sec DOUBLE,
            completed BIGINT, errored BIGINT, rejected BIGINT,
            latency_p50 DOUBLE, latency_p95 DOUBLE, latency_p99 DOUBLE, lifetime_p50 DOUBLE, lifetime_p95 DOUBLE,
            lock_waits BIGINT, instances_up INTEGER, kafka_up BOOLEAN)""",
        """
        CREATE TABLE IF NOT EXISTS perf_marks (run_id VARCHAR, ts TIMESTAMP, action VARCHAR, target VARCHAR, detail VARCHAR)""",
    };

    /**
     * Per run: settings plus a summary over its loading samples. Latency figures are over the samples' window
     * percentiles (median of the p50s, mean of the p95s, worst p99), not over individual events.
     */
    private static final String RUNS_SQL = """
        SELECT r.run_id, r.flow, r.coordinator_impl, r.machine, r.preset, r.coordinators, r.workers, r.rate, r.duration_sec,
               r.lock_mode, r.chaos, r.chaos_every_sec, r.chaos_kafka, r.tuning,
               epoch_ms(r.started_at) AS started_at, epoch_ms(r.ended_at) AS ended_at, r.status,
               count(s.ts) AS samples,
               avg(s.completed_per_sec) FILTER (WHERE s.loading) AS avg_completed_per_sec,
               max(s.completed_per_sec) AS peak_completed_per_sec,
               avg(s.processed_per_sec) FILTER (WHERE s.loading) AS avg_processed_per_sec,
               avg(s.dispatched_per_sec) FILTER (WHERE s.loading) AS avg_dispatched_per_sec,
               sum(s.completed) AS completed, sum(s.errored) AS errored, sum(s.rejected) AS rejected,
               median(s.latency_p50) FILTER (WHERE s.loading) AS latency_p50,
               avg(s.latency_p95) FILTER (WHERE s.loading) AS latency_p95,
               max(s.latency_p99) AS latency_p99_max,
               median(s.lifetime_p50) FILTER (WHERE s.loading) AS lifetime_p50,
               coalesce(m.disruptions, 0) AS disruptions
        FROM perf_runs r
        LEFT JOIN perf_samples s ON s.run_id = r.run_id
        LEFT JOIN (SELECT run_id, count(*) AS disruptions FROM perf_marks
                   WHERE action LIKE 'CHAOS %' OR action IN ('KILL', 'STOP') GROUP BY run_id) m ON m.run_id = r.run_id
        {where}
        GROUP BY ALL
        ORDER BY started_at DESC
        LIMIT ?""";

    private final Connection db;
    private final Path file;

    PerfStore(Path file) throws SQLException {
        this.file = file;
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
        } catch (java.io.IOException e) {
            throw new SQLException("cannot create " + file.getParent(), e);
        }
        this.db = DriverManager.getConnection("jdbc:duckdb:" + file.toAbsolutePath());
        try (Statement st = db.createStatement()) {
            for (String ddl : SCHEMA) {
                st.execute(ddl);
            }
            // a run still marked running belongs to a dashboard that stopped mid-run
            st.execute("UPDATE perf_runs SET status = 'interrupted' WHERE status = 'running'");
        }
    }

    Path file() {
        return file;
    }

    synchronized void startRun(String runId, String flow, String coordinatorImpl, String machine, String preset, int coordinators,
            int workers, double rate, int durationSec, String lockMode, boolean chaos, int chaosEverySec, boolean chaosKafka,
            String tuningJson, long startedAt) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("""
                INSERT INTO perf_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, make_timestamp(? * 1000), NULL, 'running')""")) {
            int i = 1;
            ps.setString(i++, runId);
            ps.setString(i++, flow);
            ps.setString(i++, coordinatorImpl);
            ps.setString(i++, machine);
            ps.setString(i++, preset);
            ps.setInt(i++, coordinators);
            ps.setInt(i++, workers);
            ps.setDouble(i++, rate);
            ps.setInt(i++, durationSec);
            ps.setString(i++, lockMode);
            ps.setBoolean(i++, chaos);
            ps.setInt(i++, chaosEverySec);
            ps.setBoolean(i++, chaosKafka);
            ps.setString(i++, tuningJson);
            ps.setLong(i, startedAt);
            ps.executeUpdate();
        }
    }

    /** Chaos was turned on or off during the run. */
    synchronized void setChaos(String runId, boolean on) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("UPDATE perf_runs SET chaos = chaos OR ? WHERE run_id = ?")) {
            ps.setBoolean(1, on);
            ps.setString(2, runId);
            ps.executeUpdate();
        }
    }

    synchronized void endRun(String runId, long endedAt, String status) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement(
                "UPDATE perf_runs SET ended_at = make_timestamp(? * 1000), status = ? WHERE run_id = ? AND ended_at IS NULL")) {
            ps.setLong(1, endedAt);
            ps.setString(2, status);
            ps.setString(3, runId);
            ps.executeUpdate();
        }
    }

    synchronized void addSample(String runId, Sample s) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("""
                INSERT INTO perf_samples VALUES (?, make_timestamp(? * 1000), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")) {
            int i = 1;
            ps.setString(i++, runId);
            ps.setLong(i++, s.ts());
            ps.setInt(i++, s.tSec());
            ps.setBoolean(i++, s.loading());
            for (Object v : new Object[] {s.publishedPerSec(), s.dispatchedPerSec(), s.processedPerSec(), s.completedPerSec(),
                    s.completed(), s.errored(), s.rejected(), s.latencyP50(), s.latencyP95(), s.latencyP99(), s.lifetimeP50(),
                    s.lifetimeP95(), s.lockWaits(), s.instancesUp(), s.kafkaUp()}) {
                ps.setObject(i++, v);
            }
            ps.executeUpdate();
        }
    }

    synchronized void addMark(String runId, long ts, String action, String target, String detail) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("INSERT INTO perf_marks VALUES (?, make_timestamp(? * 1000), ?, ?, ?)")) {
            ps.setString(1, runId);
            ps.setLong(2, ts);
            ps.setString(3, action);
            ps.setString(4, target);
            ps.setString(5, detail);
            ps.executeUpdate();
        }
    }

    /** The latest runs with their summaries, newest first. */
    synchronized List<Map<String, Object>> runs(int limit) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement(RUNS_SQL.replace("{where}", ""))) {
            ps.setInt(1, limit);
            return rows(ps);
        }
    }

    /** These runs side by side: settings and summary, samples and timeline of each, in the order asked. */
    synchronized List<Map<String, Object>> compare(List<String> runIds) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String id : runIds) {
            Map<String, Object> run;
            try (PreparedStatement ps = db.prepareStatement(RUNS_SQL.replace("{where}", "WHERE r.run_id = ?"))) {
                ps.setString(1, id);
                ps.setInt(2, 1);
                List<Map<String, Object>> r = rows(ps);
                if (r.isEmpty()) {
                    throw new IllegalArgumentException("no run " + id);
                }
                run = new LinkedHashMap<>(r.getFirst());
            }
            try (PreparedStatement ps = db.prepareStatement("""
                    SELECT epoch_ms(ts) AS ts, t_sec, loading, published_per_sec, dispatched_per_sec, processed_per_sec,
                           completed_per_sec, completed, errored, rejected, latency_p50, latency_p95, latency_p99,
                           lifetime_p50, lifetime_p95, lock_waits, instances_up, kafka_up
                    FROM perf_samples WHERE run_id = ? ORDER BY ts""")) {
                ps.setString(1, id);
                run.put("series", rows(ps));
            }
            try (PreparedStatement ps = db.prepareStatement(
                    "SELECT epoch_ms(ts) AS ts, action, target, detail FROM perf_marks WHERE run_id = ? ORDER BY ts")) {
                ps.setString(1, id);
                run.put("marks", rows(ps));
            }
            out.add(run);
        }
        return out;
    }

    synchronized boolean delete(String runId) throws SQLException {
        try (Statement st = db.createStatement()) {
            st.execute("BEGIN TRANSACTION");
        }
        try {
            int n = 0;
            for (String table : List.of("perf_samples", "perf_marks", "perf_runs")) {
                try (PreparedStatement ps = db.prepareStatement("DELETE FROM " + table + " WHERE run_id = ?")) {
                    ps.setString(1, runId);
                    n = ps.executeUpdate();
                }
            }
            try (Statement st = db.createStatement()) {
                st.execute("COMMIT");
            }
            return n > 0;
        } catch (SQLException e) {
            try (Statement st = db.createStatement()) {
                st.execute("ROLLBACK");
            }
            throw e;
        }
    }

    private static List<Map<String, Object>> rows(PreparedStatement ps) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int c = 1; c <= md.getColumnCount(); c++) {
                    Object v = rs.getObject(c);
                    if (v instanceof java.math.BigInteger b) {
                        v = b.longValue();
                    } else if (v instanceof java.math.BigDecimal d) {
                        v = d.doubleValue();
                    }
                    row.put(md.getColumnLabel(c), v);
                }
                out.add(row);
            }
        }
        return out;
    }

    @Override
    public synchronized void close() {
        try {
            db.close();
        } catch (SQLException e) {
            // closing
        }
    }
}
