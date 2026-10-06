package io.concert.store;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

/** Applies db/schema.sql, one statement per transaction (DSQL forbids mixing DDL in a transaction). */
public final class SchemaInitializer {
    private SchemaInitializer() {}

    /** duplicate_table, duplicate_object, unique_violation (on pg_type during a concurrent CREATE). */
    private static final java.util.Set<String> ALREADY_EXISTS = java.util.Set.of("42P07", "42710", "23505");

    public static void apply(DataSource ds, boolean dsql) {
        String sql;
        try (InputStream in = SchemaInitializer.class.getResourceAsStream("/db/schema.sql")) {
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read schema.sql", e);
        }
        sql = sql.replace("/*ASYNC*/", dsql ? "ASYNC" : "").replace("/*JSON*/", dsql ? "text" : "jsonb");
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(true);
            // Strip comments before splitting, so a ';' inside a comment can't cut a statement.
            for (String stmt : stripComments(sql).split(";")) {
                String s = stmt.trim();
                if (s.isEmpty()) {
                    continue;
                }
                try (Statement st = c.createStatement()) {
                    st.execute(s);
                } catch (SQLException e) {
                    // Several processes may initialize a fresh database at once; IF NOT EXISTS is not
                    // atomic in Postgres, so the loser sees "already exists". The object is there: fine.
                    if (!ALREADY_EXISTS.contains(e.getSQLState())) {
                        throw e;
                    }
                }
            }
            if (!dsql) {
                migrateDataToJsonb(c);
            }
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    /** Databases created before entity data became jsonb still have a text column: convert in place. */
    private static void migrateDataToJsonb(Connection c) throws SQLException {
        try (Statement st = c.createStatement();
                var rs = st.executeQuery("SELECT data_type FROM information_schema.columns "
                        + "WHERE table_name = 'sm_state' AND column_name = 'data'")) {
            if (!rs.next() || !rs.getString(1).equals("text")) {
                return;
            }
        }
        try (Statement st = c.createStatement()) {
            // Values that are not JSON become JSON strings instead of failing the migration.
            st.execute("ALTER TABLE sm_state ALTER COLUMN data TYPE jsonb USING "
                    + "CASE WHEN data IS NULL THEN NULL WHEN data IS JSON THEN data::jsonb ELSE to_jsonb(data) END");
        }
    }

    private static String stripComments(String stmt) {
        StringBuilder out = new StringBuilder();
        for (String line : stmt.split("\n")) {
            int i = line.indexOf("--");
            out.append(i >= 0 ? line.substring(0, i) : line).append('\n');
        }
        return out.toString();
    }
}
