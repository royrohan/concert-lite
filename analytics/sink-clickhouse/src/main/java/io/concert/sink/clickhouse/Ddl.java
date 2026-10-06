package io.concert.sink.clickhouse;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * One ClickHouse object the provisioner owns, with what it needs to create or migrate it idempotently
 * (see {@link Provisioner}).
 */
public sealed interface Ddl {

    /** Prefix of the comment that carries a definition hash on queues and materialized views. */
    String HASH_PREFIX = "concert:";

    String name();

    /**
     * A MergeTree-family table: created if missing; afterwards each column is added with
     * {@code ADD COLUMN IF NOT EXISTS} (additive evolution; type changes are not migrated).
     *
     * @param columns name and type, in order
     */
    record Table(String name, List<Column> columns, String engine, String comment) implements Ddl {
        public Table {
            columns = List.copyOf(columns);
        }

        public String create() {
            StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(Sql.id(name)).append("\n(\n");
            for (int i = 0; i < columns.size(); i++) {
                sb.append("    ").append(columns.get(i).render()).append(i + 1 < columns.size() ? ",\n" : "\n");
            }
            return sb.append(")\n").append(engine).append("\nCOMMENT ").append(Sql.str(comment)).toString();
        }

        public String addColumns() {
            StringBuilder sb = new StringBuilder("ALTER TABLE ").append(Sql.id(name));
            for (int i = 0; i < columns.size(); i++) {
                sb.append(i == 0 ? "\n    " : ",\n    ").append("ADD COLUMN IF NOT EXISTS ").append(columns.get(i).render());
            }
            return sb.toString();
        }
    }

    /** A column with its rendered ClickHouse type (and optional {@code DEFAULT} expression). */
    record Column(String name, String type, String defaultExpr) {
        public Column(String name, String type) {
            this(name, type, null);
        }

        String render() {
            return Sql.id(name) + " " + type + (defaultExpr == null ? "" : " DEFAULT " + defaultExpr);
        }
    }

    /**
     * A Kafka engine table. Its definition hash is kept in the table comment; when it changes, the
     * provisioner drops the queue's materialized views (which stops consumption), recreates the queue
     * and then the views. The consumer group, and so the committed offsets, survive.
     *
     * @param body the CREATE statement without its comment
     */
    record Queue(String name, String body) implements Ddl {
        public String hash() {
            return Ddl.hash(body);
        }

        public String create() {
            return body + "\nCOMMENT " + Sql.str(HASH_PREFIX + hash());
        }
    }

    /**
     * A materialized view {@code name TO target AS select}. Its definition hash is kept in the view
     * comment and the view is recreated only when it changes. {@code backfill}, if set, re-inserts the
     * target's rows from the source's current contents after a (re)creation: targets are
     * ReplacingMergeTrees, so rows inserted twice collapse.
     *
     * @param source the table the view reads; for a {@link Queue} source the provisioner recreates all
     *     of the queue's views together (data views before the error view), since a Kafka table streams
     *     to whichever views are attached
     * @param select the SELECT; must not end with a bare table name (the comment would become its alias)
     */
    record MaterializedView(String name, String source, String target, String select, String backfill) implements Ddl {
        public String hash() {
            return Ddl.hash(target + "\n" + select);
        }

        public String create() {
            return "CREATE MATERIALIZED VIEW " + Sql.id(name) + " TO " + Sql.id(target) + " AS\n" + select
                    + "\nCOMMENT " + Sql.str(HASH_PREFIX + hash());
        }
    }

    /** A plain view, replaced on every run ({@code CREATE OR REPLACE VIEW}). */
    record View(String name, String select, String comment) implements Ddl {
        public String create() {
            return "CREATE OR REPLACE VIEW " + Sql.id(name) + " AS\n" + select + "\nCOMMENT " + Sql.str(comment);
        }
    }

    /** First 16 hex chars of SHA-256. */
    static String hash(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)), 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
