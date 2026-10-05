package io.concert.store;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Aurora DSQL uses optimistic concurrency: conflicting transactions fail at commit with SQLSTATE
 * 40001 (OC000/OC001) and must be retried. Postgres raises the same state for serialization
 * failures, so the local stand-in exercises the same path.
 */
final class OccRetry {
    private OccRetry() {}

    static final int MAX_ATTEMPTS = 6;

    @FunctionalInterface
    interface SqlCall<T> {
        T call() throws SQLException;
    }

    static <T> T run(SqlCall<T> call) {
        SQLException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return call.call();
            } catch (SQLException e) {
                if (!isRetryable(e)) {
                    throw new StoreException(e);
                }
                last = e;
                backoff(attempt);
            }
        }
        throw new StoreException(last);
    }

    static boolean isRetryable(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException s && s.getSQLState() != null
                    && (s.getSQLState().equals("40001") || s.getSQLState().equals("40P01"))) {
                return true;
            }
        }
        return false;
    }

    private static void backoff(int attempt) {
        long base = Math.min(200, 2L << attempt);
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(base / 2, base + 1));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new StoreException(new SQLException("interrupted during OCC backoff", ie));
        }
    }
}
