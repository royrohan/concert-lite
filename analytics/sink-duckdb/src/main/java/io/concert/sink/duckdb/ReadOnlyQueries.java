package io.concert.sink.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Ad-hoc SQL for the trace UI, read-only. DuckDB has no read-only connection to a database that is
 * open for writing in the same process, so:
 *
 * <ul>
 *   <li>only one statement is accepted, and it must start with SELECT, WITH, DESCRIBE, SHOW, SUMMARIZE,
 *       EXPLAIN (of a SELECT/WITH; {@code EXPLAIN ANALYZE} executes its statement) or
 *       {@code PRAGMA table_info};
 *   <li>it runs on its own connection inside a transaction that is always rolled back;
 *   <li>external access is disabled for the database ({@link DuckDbTarget}), so no file or network
 *       reads, writes or extension installs.
 * </ul>
 *
 * Results are capped at {@value #MAX_ROWS} rows and statements are cancelled after the timeout.
 */
public final class ReadOnlyQueries implements AutoCloseable {

    static final int MAX_ROWS = 5000;
    private static final Set<String> ALLOWED = Set.of("SELECT", "WITH", "DESCRIBE", "SHOW", "SUMMARIZE", "EXPLAIN", "PRAGMA");
    private static final Pattern PRAGMA_OK = Pattern.compile("(?is)PRAGMA\\s+table_info\\s*\\(.*");
    private static final Pattern EXPLAIN_OK = Pattern.compile("(?is)EXPLAIN\\s+(ANALYZE\\s+)?(SELECT|WITH|FROM)\\b.*");

    /** @param types DuckDB type names, parallel to {@code columns} */
    public record Result(List<String> columns, List<String> types, List<List<Object>> rows, long elapsedMs, boolean truncated) {}

    private final DuckDbTarget db;
    private final Duration timeout;
    private final Semaphore concurrency = new Semaphore(4);
    private final ScheduledExecutorService canceller = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "query-timeout");
        t.setDaemon(true);
        return t;
    });

    public ReadOnlyQueries(DuckDbTarget db, Duration timeout) {
        this.db = db;
        this.timeout = timeout;
    }

    /**
     * @throws IllegalArgumentException if the statement is not allowed
     * @throws SQLException if it fails (including cancellation after the timeout)
     */
    public Result run(String sql) throws SQLException {
        String stmt = validate(sql);
        long t0 = System.nanoTime();
        concurrency.acquireUninterruptibly();
        try (Connection c = db.newReadConnection()) {
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                ScheduledFuture<?> cancel = canceller.schedule(() -> {
                    try {
                        st.cancel();
                    } catch (SQLException ignored) {
                        // already finished
                    }
                }, timeout.toMillis(), TimeUnit.MILLISECONDS);
                try {
                    return read(st, stmt, t0);
                } catch (SQLException e) {
                    if (cancel.isDone()) {
                        throw new SQLException("query cancelled after " + timeout.toSeconds() + " s", e);
                    }
                    throw e;
                } finally {
                    cancel.cancel(false);
                }
            } finally {
                c.rollback();
            }
        } finally {
            concurrency.release();
        }
    }

    private static Result read(Statement st, String sql, long t0) throws SQLException {
        List<String> columns = new ArrayList<>();
        List<String> types = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();
        boolean truncated = false;
        if (st.execute(sql)) {
            try (ResultSet rs = st.getResultSet()) {
                ResultSetMetaData md = rs.getMetaData();
                int n = md.getColumnCount();
                for (int i = 1; i <= n; i++) {
                    columns.add(md.getColumnLabel(i));
                    types.add(md.getColumnTypeName(i));
                }
                while (rs.next()) {
                    if (rows.size() == MAX_ROWS) {
                        truncated = true;
                        break;
                    }
                    List<Object> row = new ArrayList<>(n);
                    for (int i = 1; i <= n; i++) {
                        row.add(jsonValue(rs.getObject(i)));
                    }
                    rows.add(row);
                }
            }
        }
        return new Result(columns, types, rows, (System.nanoTime() - t0) / 1_000_000, truncated);
    }

    /** Numbers and booleans as JSON values; everything else (timestamps, JSON, lists, structs) as text. */
    private static Object jsonValue(Object v) {
        return switch (v) {
            case null -> null;
            case Boolean b -> b;
            case Double d -> d.isNaN() || d.isInfinite() ? d.toString() : d;
            case Float f -> f.isNaN() || f.isInfinite() ? f.toString() : f;
            case Long l -> l;
            case Integer i -> i;
            case Short s -> s;
            case Byte b -> b;
            case BigDecimal d -> d;
            case BigInteger i -> i;
            default -> v.toString();
        };
    }

    /** The single allowed statement, without trailing semicolons; throws if it is not allowed. */
    static String validate(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("empty query");
        }
        String s = stripLeadingComments(sql.strip());
        while (s.endsWith(";")) {
            s = s.substring(0, s.length() - 1).stripTrailing();
        }
        if (hasUnquotedSemicolon(s)) {
            throw new IllegalArgumentException("only one statement per query");
        }
        String first = s.split("[\\s(]+", 2)[0].toUpperCase(Locale.ROOT);
        boolean ok = ALLOWED.contains(first)
                && (!first.equals("PRAGMA") || PRAGMA_OK.matcher(s).matches())
                && (!first.equals("EXPLAIN") || EXPLAIN_OK.matcher(s).matches());
        if (!ok) {
            throw new IllegalArgumentException("read-only endpoint: only SELECT, WITH, DESCRIBE, SHOW, SUMMARIZE, "
                    + "EXPLAIN SELECT and PRAGMA table_info(...) are allowed");
        }
        return s;
    }

    private static String stripLeadingComments(String s) {
        String r = s;
        while (true) {
            if (r.startsWith("--")) {
                int nl = r.indexOf('\n');
                r = nl < 0 ? "" : r.substring(nl + 1).strip();
            } else if (r.startsWith("/*")) {
                int end = r.indexOf("*/");
                r = end < 0 ? "" : r.substring(end + 2).strip();
            } else {
                return r;
            }
        }
    }

    /** A {@code ;} outside string literals, quoted identifiers and comments. */
    private static boolean hasUnquotedSemicolon(String s) {
        char quote = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (c == '-' && i + 1 < s.length() && s.charAt(i + 1) == '-') {
                int nl = s.indexOf('\n', i);
                if (nl < 0) {
                    return false;
                }
                i = nl;
            } else if (c == '/' && i + 1 < s.length() && s.charAt(i + 1) == '*') {
                int end = s.indexOf("*/", i + 2);
                if (end < 0) {
                    return false;
                }
                i = end + 1;
            } else if (c == ';') {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        canceller.shutdownNow();
    }
}
