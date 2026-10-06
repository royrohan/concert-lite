package io.concert.sink.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Offline maintenance of a sink database file, for tests and reconciliation drills only. It opens the
 * file directly, so the sink must be <b>stopped</b> (DuckDB allows one writing process); it is not
 * reachable over HTTP.
 *
 * <pre>
 * docker compose stop sink-duckdb
 * docker compose run --rm --no-deps --entrypoint java sink-duckdb -cp '/app/lib/*' \
 *     io.concert.sink.duckdb.DuckDbAdmin /data/concert.duckdb delete order:o-123
 * docker compose start sink-duckdb
 * </pre>
 *
 * {@code delete <entityId>} removes the entity's root row and child rows (every table with an
 * {@code entity_id} column) without touching the stored Kafka offsets, so the sink does not re-read
 * it: only reconciliation brings it back.
 */
public final class DuckDbAdmin {

    private DuckDbAdmin() {}

    public static void main(String[] args) throws SQLException {
        if (args.length != 3 || !args[1].equals("delete")) {
            System.err.println("usage: DuckDbAdmin <file.duckdb> delete <entityId>");
            System.exit(2);
        }
        System.out.println(delete(args[0], args[2]) + " row(s) deleted for " + args[2]);
    }

    /** Deletes the entity's rows from every table with an {@code entity_id} column; returns the row count. */
    static int delete(String file, String entityId) throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:" + file)) {
            List<String> tables = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT table_name FROM information_schema.columns"
                    + " WHERE table_schema = 'main' AND column_name = 'entity_id'");
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
            int n = 0;
            for (String t : tables) {
                try (PreparedStatement del = c.prepareStatement("DELETE FROM " + DuckDbTarget.q(t) + " WHERE entity_id = ?")) {
                    del.setString(1, entityId);
                    n += del.executeUpdate();
                }
            }
            return n;
        }
    }
}
