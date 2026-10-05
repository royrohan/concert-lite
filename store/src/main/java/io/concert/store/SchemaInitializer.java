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
        sql = sql.replace("/*ASYNC*/", dsql ? "ASYNC" : "");
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(true);
            for (String stmt : sql.split(";")) {
                String s = stripComments(stmt).trim();
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
        } catch (SQLException e) {
            throw new StoreException(e);
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
