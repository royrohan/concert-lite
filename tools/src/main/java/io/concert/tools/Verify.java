package io.concert.tools;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Json;
import io.concert.store.JdbcStateStore;
import io.concert.store.Stores;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.HdrHistogram.Histogram;

/**
 * Checks a chaos run against its manifest, straight from the DSQL stand-in:
 *
 * <ul>
 *   <li><b>no loss</b>: every ledger's applied count == events sent to it,
 *   <li><b>no double apply</b>: never more than sent,
 *   <li><b>order</b>: no event applied after a higher seq on the same ledger,
 *   <li><b>dedupe state</b>: every event reached DISPATCHED.
 * </ul>
 *
 * Then prints throughput per 5 s bucket with the chaos actions marked, and producer->done latency.
 */
final class Verify {
    private Verify() {}

    private static final Pattern SRC = Pattern.compile("srcMs=(-?\\d+)");
    private static final DateTimeFormatter HMS = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    record ChaosEvent(long ts, String action, String target) {}

    static int run(Map<String, String> o) throws Exception {
        System.setProperty("STORE_INIT_SCHEMA", "false");
        Manifest m = Json.read(Files.readString(Path.of(o.get("manifest"))), Manifest.class);
        List<ChaosEvent> chaos = readChaos(o.get("chaos-log"));
        int waitSeconds = Integer.parseInt(o.getOrDefault("wait-seconds", "300"));

        try (JdbcStateStore store = Stores.fromEnv()) {
            long deadline = System.currentTimeMillis() + waitSeconds * 1000L;
            Map<String, long[]> state;
            long applied;
            while (true) {
                state = ledgerState(store, m.runId());
                applied = state.values().stream().mapToLong(v -> v[0]).sum();
                if (applied >= m.total() || System.currentTimeMillis() > deadline) {
                    break;
                }
                System.out.printf("[verify] draining: applied %d/%d%n", applied, m.total());
                Thread.sleep(5000);
            }

            int lostKeys = 0, overKeys = 0, oooKeys = 0;
            long lost = 0, over = 0, ooo = 0;
            for (int k = 0; k < m.keys(); k++) {
                long[] s = state.getOrDefault("ledger:" + m.runId() + "-K" + k, new long[] {0, 0});
                long expected = m.perKey()[k];
                if (s[0] < expected) {
                    lostKeys++;
                    lost += expected - s[0];
                }
                if (s[0] > expected) {
                    overKeys++;
                    over += s[0] - expected;
                }
                if (s[1] > 0) {
                    oooKeys++;
                    ooo += s[1];
                }
            }
            Map<String, Long> dedupe = statusCounts(store, m.runId());
            long dropped = count(store, "SELECT count(*) FROM event_trace WHERE stage = 'DROPPED' AND event_id LIKE ?", m.runId() + "-%");
            long failed = count(store, "SELECT count(*) FROM event_trace WHERE stage = 'FAILED' AND event_id LIKE ?", m.runId() + "-%");

            System.out.println();
            System.out.println("================ chaos verification: " + m.runId() + " ================");
            System.out.printf("sent %d events to %d ledgers (%d single-key, %d multi-key) at %.0f/s%n", m.total(), m.keys(),
                    (m.keys() + 1) / 2, m.keys() / 2, m.total() * 1000.0 / Math.max(1, m.finishedAt() - m.startedAt()));
            System.out.printf("chaos actions: %d (%s)%n", chaos.stream().filter(c -> !c.action().equals("START")).count(),
                    summarize(chaos));
            System.out.println();
            check("no lost events", lost == 0, lost + " missing across " + lostKeys + " ledgers");
            check("no double-applied events", over == 0, over + " extra across " + overKeys + " ledgers");
            check("per-key order kept", ooo == 0, ooo + " out-of-order applies across " + oooKeys + " ledgers");
            check("dedupe state settled", dedupe.getOrDefault("RECEIVED", 0L) == 0, "status counts " + dedupe);
            System.out.printf("  info  replays absorbed by dedupe (DROPPED rows): %d, FAILED rows: %d%n", dropped, failed);
            System.out.println();
            latency(store, m.runId());
            timeline(store, m, chaos);
            boolean pass = lost == 0 && over == 0 && ooo == 0;
            System.out.println(pass ? "RESULT: PASS" : "RESULT: FAIL");
            return pass ? 0 : 1;
        }
    }

    private static void check(String what, boolean ok, String detail) {
        System.out.printf("  %s  %s%s%n", ok ? "PASS" : "FAIL", what, ok ? "" : " — " + detail);
    }

    /** workflow_id -> [count, outOfOrder] from the ledger projections. */
    private static Map<String, long[]> ledgerState(JdbcStateStore store, String runId) throws SQLException {
        Map<String, long[]> out = new HashMap<>();
        try (Connection c = store.dataSource().getConnection();
                PreparedStatement ps = c.prepareStatement("SELECT workflow_id, data FROM sm_state WHERE workflow_id LIKE ?")) {
            ps.setString(1, "ledger:" + runId + "-K%");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    JsonNode d = Json.read(rs.getString(2), JsonNode.class);
                    out.put(rs.getString(1), new long[] {d.path("count").asLong(), d.path("outOfOrder").asLong()});
                }
            }
        }
        return out;
    }

    private static Map<String, Long> statusCounts(JdbcStateStore store, String runId) throws SQLException {
        Map<String, Long> out = new TreeMap<>();
        try (Connection c = store.dataSource().getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT status, count(*) FROM processed_event WHERE event_id LIKE ? GROUP BY status")) {
            ps.setString(1, runId + "-%");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), rs.getLong(2));
                }
            }
        }
        return out;
    }

    private static long count(JdbcStateStore store, String sql, String arg) throws SQLException {
        try (Connection c = store.dataSource().getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, arg);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static void latency(JdbcStateStore store, String runId) throws SQLException {
        Histogram h = new Histogram(TimeUnit.HOURS.toMillis(1), 3);
        try (Connection c = store.dataSource().getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT min(detail) FROM event_trace WHERE stage = 'DONE' AND event_id LIKE ? GROUP BY event_id")) {
            ps.setString(1, runId + "-%");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Matcher mt = SRC.matcher(rs.getString(1));
                    if (mt.find()) {
                        h.recordValue(Math.max(0, Long.parseLong(mt.group(1))));
                    }
                }
            }
        }
        System.out.printf("producer->done latency under chaos: p50=%dms p90=%dms p99=%dms max=%dms%n%n",
                h.getValueAtPercentile(50), h.getValueAtPercentile(90), h.getValueAtPercentile(99), h.getMaxValue());
    }

    /** Completed events per 5 s, with chaos actions marked in the bucket they happened in. */
    private static void timeline(JdbcStateStore store, Manifest m, List<ChaosEvent> chaos) throws SQLException {
        TreeMap<Long, Long> buckets = new TreeMap<>();
        try (Connection c = store.dataSource().getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT floor(extract(epoch FROM t) / 5)::bigint * 5000, count(*) FROM "
                                + "(SELECT min(ts) t FROM event_trace WHERE stage = 'DONE' AND event_id LIKE ? GROUP BY event_id) d "
                                + "GROUP BY 1 ORDER BY 1")) {
            ps.setString(1, m.runId() + "-%");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    buckets.put(rs.getLong(1), rs.getLong(2));
                }
            }
        }
        if (buckets.isEmpty()) {
            return;
        }
        long first = Math.min(buckets.firstKey(), m.startedAt() / 5000 * 5000);
        long last = Math.max(buckets.lastKey(), chaos.isEmpty() ? 0 : chaos.get(chaos.size() - 1).ts() / 5000 * 5000);
        long max = buckets.values().stream().mapToLong(Long::longValue).max().orElse(1);
        System.out.println("completed events per 5s (each # = " + Math.max(1, max / 40) + " events)");
        for (long b = first; b <= last; b += 5000) {
            long n = buckets.getOrDefault(b, 0L);
            long bucket = b;
            StringBuilder marks = new StringBuilder();
            for (ChaosEvent ce : chaos) {
                if (ce.ts() >= bucket && ce.ts() < bucket + 5000) {
                    marks.append(" <- ").append(ce.action()).append(' ').append(ce.target());
                }
            }
            System.out.printf("%s %5d/s %-40s%s%n", HMS.format(Instant.ofEpochMilli(b)), n / 5,
                    "#".repeat((int) (n / Math.max(1, max / 40))), marks);
        }
        System.out.println();
    }

    private static List<ChaosEvent> readChaos(String path) throws Exception {
        List<ChaosEvent> out = new ArrayList<>();
        if (path == null || !Files.exists(Path.of(path))) {
            return out;
        }
        for (String line : Files.readAllLines(Path.of(path))) {
            String[] p = line.trim().split("\\s+", 3);
            if (p.length >= 2 && p[0].matches("\\d+")) {
                out.add(new ChaosEvent(Long.parseLong(p[0]), p[1], p.length > 2 ? p[2] : ""));
            }
        }
        return out;
    }

    private static String summarize(List<ChaosEvent> chaos) {
        Map<String, Integer> byAction = new TreeMap<>();
        chaos.forEach(c -> byAction.merge(c.action(), 1, Integer::sum));
        return byAction.toString();
    }
}
