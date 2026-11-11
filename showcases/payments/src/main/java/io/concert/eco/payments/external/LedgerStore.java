package io.concert.eco.payments.external;

import io.concert.common.Json;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Where {@link LedgerService} keeps its charges and the answers stored per idempotency key. A charge and its stored
 * answer are committed together, so a crash between them is impossible: after a restart a key is either unknown (the
 * retry commits it) or answered from the store (the retry replays it).
 */
interface LedgerStore {

    /** Charges over this amount are declined. */
    BigDecimal LIMIT = new BigDecimal("10000");

    /** An HTTP answer: status code, JSON body, and whether it was the stored answer to a repeated key. */
    record Answer(int code, String body, boolean replayed) {}

    /** Counters kept per payment: chargeRequests, replays, faults, refundRequests (commits are counted by the store). */
    void count(String paymentId, String counter);

    /** Commits a charge (or a decline) once per key; with {@code idempotency} off, every call commits again. */
    Answer charge(String key, String paymentId, BigDecimal amount, boolean idempotency);

    /** Refunds the payment's live charges once per key; nothing to refund is an answer too. */
    Answer refund(String key, String paymentId, boolean idempotency);

    /** Per payment with the prefix: counters and entries (what {@code GET /charges?prefix=} returns). */
    List<Map<String, Object>> list(String prefix);

    static String declined(BigDecimal amount) {
        return Json.write(Map.of("error", "insufficient funds: " + amount + " is over the " + LIMIT + " limit"));
    }

    // ---------------------------------------------------------------------------------------------------- in memory

    /** In memory: tests and host runs. A restart forgets everything. */
    final class Memory implements LedgerStore {
        private final Map<String, Answer> answers = new ConcurrentHashMap<>();
        private final Map<String, List<Map<String, String>>> entries = new ConcurrentHashMap<>();
        private final Map<String, Map<String, AtomicLong>> counters = new ConcurrentHashMap<>();
        private final AtomicLong seq = new AtomicLong();

        @Override
        public void count(String paymentId, String counter) {
            counters.computeIfAbsent(paymentId, k -> new ConcurrentHashMap<>()).computeIfAbsent(counter, k -> new AtomicLong())
                    .incrementAndGet();
        }

        @Override
        public synchronized Answer charge(String key, String paymentId, BigDecimal amount, boolean idempotency) {
            Answer stored = idempotency && key != null ? answers.get(key) : null;
            if (stored != null) {
                count(paymentId, "replays");
                return new Answer(stored.code(), stored.body(), true);
            }
            String ref = "LDG-" + seq.incrementAndGet();
            boolean over = amount.compareTo(LIMIT) > 0;
            Answer a = over ? new Answer(422, declined(amount), false)
                    : new Answer(201, Json.write(Map.of("ledgerRef", ref, "status", "CHARGED")), false);
            entries.computeIfAbsent(paymentId, k -> new CopyOnWriteArrayList<>())
                    .add(new ConcurrentHashMap<>(Map.of("ledgerRef", ref, "status", over ? "DECLINED" : "CHARGED")));
            count(paymentId, over ? "declines" : "chargeCommits");
            if (idempotency && key != null) {
                answers.put(key, a);
            }
            return a;
        }

        @Override
        public synchronized Answer refund(String key, String paymentId, boolean idempotency) {
            Answer stored = idempotency && key != null ? answers.get(key) : null;
            if (stored != null) {
                count(paymentId, "replays");
                return new Answer(stored.code(), stored.body(), true);
            }
            List<Map<String, String>> live = entries.getOrDefault(paymentId, List.of()).stream()
                    .filter(e -> e.get("status").equals("CHARGED")).toList();
            live.forEach(e -> {
                e.put("status", "REFUNDED");
                count(paymentId, "refunds");
            });
            Answer a = new Answer(200, live.isEmpty() ? Json.write(Map.of("status", "NOTHING_TO_REFUND"))
                    : Json.write(Map.of("ledgerRef", live.getFirst().get("ledgerRef"), "status", "REFUNDED")), false);
            if (idempotency && key != null) {
                answers.put(key, a);
            }
            return a;
        }

        @Override
        public List<Map<String, Object>> list(String prefix) {
            List<Map<String, Object>> out = new ArrayList<>();
            counters.keySet().stream().filter(p -> p.startsWith(prefix)).sorted().forEach(p -> {
                Map<String, Object> m = row(p, name -> counters.get(p).getOrDefault(name, new AtomicLong()).get());
                m.put("entries", List.copyOf(entries.getOrDefault(p, List.of())));
                out.add(m);
            });
            return out;
        }
    }

    String[] COUNTERS = {"chargeRequests", "chargeCommits", "replays", "declines", "refundRequests", "refunds", "faults"};

    static Map<String, Object> row(String paymentId, java.util.function.ToLongFunction<String> counter) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("paymentId", paymentId);
        for (String c : COUNTERS) {
            m.put(c, counter.applyAsLong(c));
        }
        return m;
    }

    // ----------------------------------------------------------------------------------------------------- Postgres

    /**
     * In Postgres ({@code ledger_*} tables): survives restarts. Each charge or refund is one transaction that takes a
     * per-key advisory lock, looks up the stored answer, and otherwise writes the entry and the answer together.
     */
    final class Postgres implements LedgerStore {
        private final String url, user, password;

        Postgres(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
            schema();
        }

        private Connection connect() throws SQLException {
            return DriverManager.getConnection(url, user, password);
        }

        /** Creates the tables, waiting for the database to accept connections (it may start after the ledger). */
        private void schema() {
            for (int attempt = 1; ; attempt++) {
                try (Connection c = connect(); Statement s = c.createStatement()) {
                    s.execute("CREATE TABLE IF NOT EXISTS ledger_answers (idem_key TEXT PRIMARY KEY, payment_id TEXT NOT NULL, "
                            + "code INT NOT NULL, body TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
                    s.execute("CREATE TABLE IF NOT EXISTS ledger_entries (id BIGSERIAL PRIMARY KEY, payment_id TEXT NOT NULL, "
                            + "status TEXT NOT NULL, idem_key TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
                    s.execute("CREATE INDEX IF NOT EXISTS ledger_entries_payment ON ledger_entries (payment_id)");
                    s.execute("CREATE TABLE IF NOT EXISTS ledger_counters (payment_id TEXT NOT NULL, name TEXT NOT NULL, "
                            + "n BIGINT NOT NULL, PRIMARY KEY (payment_id, name))");
                    return;
                } catch (SQLException e) {
                    if (attempt >= 60) {
                        throw new IllegalStateException("ledger database not reachable at " + url, e);
                    }
                    sleep(1000);
                }
            }
        }

        @Override
        public void count(String paymentId, String counter) {
            try (Connection c = connect()) {
                count(c, paymentId, counter, 1);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        private static void count(Connection c, String paymentId, String counter, int n) throws SQLException {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO ledger_counters (payment_id, name, n) VALUES (?, ?, ?) "
                    + "ON CONFLICT (payment_id, name) DO UPDATE SET n = ledger_counters.n + EXCLUDED.n")) {
                ps.setString(1, paymentId);
                ps.setString(2, counter);
                ps.setInt(3, n);
                ps.executeUpdate();
            }
        }

        /** Runs {@code body} in one transaction holding the advisory lock of {@code key} (if any). */
        private <T> T inTx(String key, SqlBody<T> body) {
            try (Connection c = connect()) {
                c.setAutoCommit(false);
                try {
                    if (key != null) {
                        try (PreparedStatement ps = c.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?))")) {
                            ps.setString(1, key);
                            ps.execute();
                        }
                    }
                    T t = body.run(c);
                    c.commit();
                    return t;
                } catch (SQLException | RuntimeException e) {
                    c.rollback();
                    throw e;
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        private interface SqlBody<T> { T run(Connection c) throws SQLException; }

        private static Answer stored(Connection c, String key) throws SQLException {
            try (PreparedStatement ps = c.prepareStatement("SELECT code, body FROM ledger_answers WHERE idem_key = ?")) {
                ps.setString(1, key);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? new Answer(rs.getInt(1), rs.getString(2), true) : null;
                }
            }
        }

        private static void store(Connection c, String key, String paymentId, Answer a) throws SQLException {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO ledger_answers (idem_key, payment_id, code, body) VALUES (?, ?, ?, ?)")) {
                ps.setString(1, key);
                ps.setString(2, paymentId);
                ps.setInt(3, a.code());
                ps.setString(4, a.body());
                ps.executeUpdate();
            }
        }

        @Override
        public Answer charge(String key, String paymentId, BigDecimal amount, boolean idempotency) {
            boolean keyed = idempotency && key != null;
            return inTx(keyed ? key : null, c -> {
                if (keyed) {
                    Answer s = stored(c, key);
                    if (s != null) {
                        count(c, paymentId, "replays", 1);
                        return s;
                    }
                }
                boolean over = amount.compareTo(LIMIT) > 0;
                long id;
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO ledger_entries (payment_id, status, idem_key) VALUES (?, ?, ?) RETURNING id")) {
                    ps.setString(1, paymentId);
                    ps.setString(2, over ? "DECLINED" : "CHARGED");
                    ps.setString(3, key);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        id = rs.getLong(1);
                    }
                }
                count(c, paymentId, over ? "declines" : "chargeCommits", 1);
                Answer a = over ? new Answer(422, declined(amount), false)
                        : new Answer(201, Json.write(Map.of("ledgerRef", "LDG-" + id, "status", "CHARGED")), false);
                if (keyed) {
                    store(c, key, paymentId, a);
                }
                return a;
            });
        }

        @Override
        public Answer refund(String key, String paymentId, boolean idempotency) {
            boolean keyed = idempotency && key != null;
            return inTx(keyed ? key : null, c -> {
                if (keyed) {
                    Answer s = stored(c, key);
                    if (s != null) {
                        count(c, paymentId, "replays", 1);
                        return s;
                    }
                }
                List<Long> ids = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement(
                        "UPDATE ledger_entries SET status = 'REFUNDED' WHERE payment_id = ? AND status = 'CHARGED' RETURNING id")) {
                    ps.setString(1, paymentId);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            ids.add(rs.getLong(1));
                        }
                    }
                }
                if (!ids.isEmpty()) {
                    count(c, paymentId, "refunds", ids.size());
                }
                Answer a = new Answer(200, ids.isEmpty() ? Json.write(Map.of("status", "NOTHING_TO_REFUND"))
                        : Json.write(Map.of("ledgerRef", "LDG-" + ids.getFirst(), "status", "REFUNDED")), false);
                if (keyed) {
                    store(c, key, paymentId, a);
                }
                return a;
            });
        }

        @Override
        public List<Map<String, Object>> list(String prefix) {
            String like = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%";
            Map<String, Map<String, Long>> counters = new LinkedHashMap<>();
            Map<String, List<Map<String, String>>> entries = new LinkedHashMap<>();
            try (Connection c = connect()) {
                try (PreparedStatement ps = c.prepareStatement(
                        "SELECT payment_id, name, n FROM ledger_counters WHERE payment_id LIKE ? ORDER BY payment_id")) {
                    ps.setString(1, like);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            counters.computeIfAbsent(rs.getString(1), k -> new LinkedHashMap<>()).put(rs.getString(2), rs.getLong(3));
                        }
                    }
                }
                try (PreparedStatement ps = c.prepareStatement(
                        "SELECT payment_id, id, status FROM ledger_entries WHERE payment_id LIKE ? ORDER BY id")) {
                    ps.setString(1, like);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            entries.computeIfAbsent(rs.getString(1), k -> new ArrayList<>())
                                    .add(Map.of("ledgerRef", "LDG-" + rs.getLong(2), "status", rs.getString(3)));
                        }
                    }
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
            List<Map<String, Object>> out = new ArrayList<>();
            counters.forEach((p, m) -> {
                Map<String, Object> row = row(p, name -> m.getOrDefault(name, 0L));
                row.put("entries", entries.getOrDefault(p, List.of()));
                out.add(row);
            });
            return out;
        }

        private static void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
