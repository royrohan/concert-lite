package io.concert.sink.duckdb;

import io.concert.sink.ColumnSpec;
import io.concert.sink.ColumnType;
import io.concert.sink.EntitySnapshot;
import io.concert.sink.SchemaSpec;
import io.concert.sink.SinkTarget;
import io.concert.sink.SinkTarget.RecordException;
import io.concert.sink.SnapshotRecord;
import io.concert.sink.SnapshotRows;
import io.concert.sink.TableSpec;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the latest snapshot of every completed entity in a DuckDB file: one table per root, one per
 * owned to-many association (see {@code SchemaMapper}), value classes flattened into prefixed
 * columns ({@code total_amount}, {@code total_currency}).
 *
 * <p>{@link #apply} runs in <b>one transaction</b>: per entity whose version advanced, upsert the root
 * row ({@code ON CONFLICT (entity_id) DO UPDATE ... WHERE excluded.entity_version >
 * <table>.entity_version}) and replace its child rows (delete + insert); then upsert the next offset
 * per partition into {@code _sink_offsets}; then commit. Applying a snapshot twice, or an older one,
 * changes nothing, and the rows and offsets are never out of step, so with the consumer resuming from
 * {@link #storedOffsets()} each snapshot is applied effectively once.
 *
 * <p>This process is the only writer of the file. Read connections for queries come from
 * {@link #newReadConnection()} (same database instance). External access (files, HTTP, extension
 * installs) is disabled for the whole database, so ad-hoc queries cannot read the host's files.
 * Decimals are stored as DECIMAL(38,4); more fractional digits are rounded.
 */
public final class DuckDbTarget implements SinkTarget {
    private static final Logger log = LoggerFactory.getLogger(DuckDbTarget.class);

    static final String OFFSETS_TABLE = "_sink_offsets";
    /** DuckDB's timestamp literal format (OffsetDateTime.toString drops zero seconds, which DuckDB rejects). */
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSXXX");

    /** Counters for {@code /health}. */
    public record Stats(long appliedEntities, long staleSkipped, Map<String, Long> unknownSmTypes) {}

    private final Connection conn;
    private final Map<String, PreparedStatement> statements = new HashMap<>();
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong stale = new AtomicLong();
    private final Map<String, Long> unknown = new ConcurrentHashMap<>();
    private volatile SchemaSpec schema;
    /** Test seam: runs after all writes of a batch, before the commit. */
    volatile Runnable beforeCommit = () -> {};

    /** @param path the database file; {@code ""} for an in-memory database */
    public DuckDbTarget(String path) {
        try {
            Properties p = new Properties();
            p.setProperty("enable_external_access", "false");
            conn = DriverManager.getConnection("jdbc:duckdb:" + path, p);
            try (Statement st = conn.createStatement()) {
                st.execute("SET TimeZone = 'UTC'");
            }
        } catch (SQLException e) {
            throw new IllegalStateException("cannot open DuckDB " + path + ": " + e.getMessage(), e);
        }
    }

    /** A new connection to the same database, for readers (each caller owns and closes it). */
    public synchronized Connection newReadConnection() throws SQLException {
        return ((DuckDBConnection) conn).duplicate();
    }

    public SchemaSpec schema() {
        return schema;
    }

    public Stats stats() {
        return new Stats(applied.get(), stale.get(), Map.copyOf(unknown));
    }

    @Override
    public synchronized void ensureSchema(SchemaSpec spec) {
        try (Statement st = conn.createStatement()) {
            for (TableSpec t : spec.tables()) {
                st.execute(createTable(t));
                // Columns added to a model after the table was created; type changes are not migrated.
                for (ColumnSpec c : t.columns()) {
                    st.execute("ALTER TABLE " + q(t.name()) + " ADD COLUMN IF NOT EXISTS " + q(c.name()) + " " + sqlType(c.type()));
                }
            }
            st.execute("CREATE TABLE IF NOT EXISTS " + OFFSETS_TABLE + " (topic VARCHAR NOT NULL, \"partition\" INTEGER NOT NULL,"
                    + " next_offset BIGINT NOT NULL, updated_at TIMESTAMPTZ, PRIMARY KEY (topic, \"partition\"))");
        } catch (SQLException e) {
            throw new IllegalStateException("creating tables failed: " + e.getMessage(), e);
        }
        schema = spec;
        log.info("DuckDB tables: {}", spec.tables().stream().map(TableSpec::name).toList());
    }

    static String createTable(TableSpec t) {
        String cols = t.columns().stream()
                .map(c -> q(c.name()) + " " + sqlType(c.type())
                        + (t.kind() == TableSpec.Kind.ROOT && c.source() == ColumnSpec.Source.ENTITY_ID ? " PRIMARY KEY" : ""))
                .collect(Collectors.joining(", "));
        // Child tables have no key: their rows are deleted and re-inserted per version.
        return "CREATE TABLE IF NOT EXISTS " + q(t.name()) + " (" + cols + ")";
    }

    static String sqlType(ColumnType type) {
        return switch (type) {
            case STRING, ENUM -> "VARCHAR";
            case BIGINT -> "BIGINT";
            case DOUBLE -> "DOUBLE";
            case DECIMAL -> "DECIMAL(38,4)";
            case BOOLEAN -> "BOOLEAN";
            case DATE -> "DATE";
            case TIMESTAMPTZ -> "TIMESTAMPTZ";
            case JSON -> "JSON";
        };
    }

    @Override
    public void apply(List<SnapshotRecord> batch) {
        apply(batch, Map.of());
    }

    /**
     * Applies {@code batch} and stores the next offsets of its records merged with {@code nextOffsets}
     * (which also covers records the loop skipped or dead-lettered), all in one transaction. A snapshot
     * whose data cannot be converted to its table's columns fails the batch with a
     * {@link SinkTarget.RecordException} naming it.
     */
    @Override
    public synchronized void apply(List<SnapshotRecord> batch, Map<TopicPartition, Long> nextOffsets) {
        SchemaSpec spec = schema;
        if (spec == null) {
            throw new IllegalStateException("ensureSchema was not called");
        }
        // Only the highest version per entity matters (ties: the later record).
        Map<String, SnapshotRecord> latest = new LinkedHashMap<>();
        Map<TopicPartition, Long> next = new HashMap<>(nextOffsets);
        for (SnapshotRecord r : batch) {
            latest.merge(r.snapshot().entityId(), r, (a, b) -> b.snapshot().version() >= a.snapshot().version() ? b : a);
            next.merge(r.topicPartition(), r.offset() + 1, Math::max);
        }
        long appliedHere = 0;
        long staleHere = 0;
        Map<String, Long> unknownHere = new HashMap<>();
        try {
            conn.setAutoCommit(false);
            try {
                for (SnapshotRecord rec : latest.values()) {
                    EntitySnapshot s = rec.snapshot();
                    TableSpec root = spec.rootFor(s.smType()).orElse(null);
                    if (root == null) {
                        unknownHere.merge(s.smType(), 1L, Long::sum);
                        continue;
                    }
                    Long current = currentVersion(root, s.entityId());
                    if (current != null && current >= s.version()) {
                        staleHere++;
                        continue;
                    }
                    try {
                        SnapshotRows.EntityRows rows = SnapshotRows.extract(spec, root, s);
                        upsertRoot(root, rows.rootRow());
                        for (var child : rows.children().entrySet()) {
                            replaceChildren(child.getKey(), s.entityId(), child.getValue());
                        }
                    } catch (RuntimeException e) {
                        throw new RecordException(rec, "snapshot " + s.entityId() + " v" + s.version() + " does not fit table "
                                + root.name(), e);
                    } catch (SQLException e) {
                        if (isDataError(e)) {
                            throw new RecordException(rec, "snapshot " + s.entityId() + " v" + s.version()
                                    + " rejected by DuckDB", e);
                        }
                        throw e;
                    }
                    appliedHere++;
                }
                storeOffsets(next);
                beforeCommit.run();
                conn.commit();
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                // DuckDB closes a statement that failed; drop the cache so the retry prepares fresh ones.
                closeStatements();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("DuckDB apply failed: " + e.getMessage(), e);
        }
        applied.addAndGet(appliedHere);
        stale.addAndGet(staleHere);
        unknownHere.forEach((type, n) -> {
            if (unknown.merge(type, n, Long::sum) == n) {
                log.warn("skipping snapshots of smType '{}': not in SINK_ROOTS", type);
            }
        });
    }

    /** DuckDB errors caused by the values of one row (not by the database's state). */
    static boolean isDataError(SQLException e) {
        String m = String.valueOf(e.getMessage());
        return m.contains("Conversion Error") || m.contains("Invalid Input Error") || m.contains("Out of Range Error")
                || m.contains("Constraint Error");
    }

    private Long currentVersion(TableSpec root, String entityId) throws SQLException {
        PreparedStatement ps = prepare("SELECT entity_version FROM " + q(root.name()) + " WHERE entity_id = ?");
        ps.setString(1, entityId);
        try (ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : null;
        }
    }

    private void upsertRoot(TableSpec root, Object[] row) throws SQLException {
        PreparedStatement ps = prepare(upsertSql(root));
        bind(ps, root.columns(), row);
        ps.executeUpdate();
    }

    private void replaceChildren(TableSpec child, String entityId, List<Object[]> rows) throws SQLException {
        PreparedStatement del = prepare("DELETE FROM " + q(child.name()) + " WHERE entity_id = ?");
        del.setString(1, entityId);
        del.executeUpdate();
        if (rows.isEmpty()) {
            return;
        }
        PreparedStatement ins = prepare("INSERT INTO " + q(child.name()) + " ("
                + child.columns().stream().map(c -> q(c.name())).collect(Collectors.joining(", ")) + ") VALUES ("
                + child.columns().stream().map(c -> placeholder(c.type())).collect(Collectors.joining(", ")) + ")");
        for (Object[] row : rows) {
            bind(ins, child.columns(), row);
            ins.executeUpdate();
        }
    }

    private void storeOffsets(Map<TopicPartition, Long> next) throws SQLException {
        PreparedStatement ps = prepare("INSERT INTO " + OFFSETS_TABLE + " VALUES (?, ?, ?, current_timestamp)"
                + " ON CONFLICT (topic, \"partition\") DO UPDATE SET next_offset = greatest(excluded.next_offset, "
                + OFFSETS_TABLE + ".next_offset), updated_at = excluded.updated_at");
        for (var e : next.entrySet()) {
            ps.setString(1, e.getKey().topic());
            ps.setInt(2, e.getKey().partition());
            ps.setLong(3, e.getValue());
            ps.executeUpdate();
        }
    }

    /**
     * Stored {@code entity_version} per entity id of {@code root} (ids without a row are absent), on a
     * read connection: for reconciliation, never writes.
     */
    public Map<String, Long> versions(TableSpec root, List<String> entityIds) throws SQLException {
        Map<String, Long> out = new LinkedHashMap<>();
        try (Connection c = newReadConnection()) {
            for (int from = 0; from < entityIds.size(); from += 500) {
                List<String> chunk = entityIds.subList(from, Math.min(entityIds.size(), from + 500));
                String sql = "SELECT entity_id, entity_version FROM " + q(root.name()) + " WHERE entity_id IN ("
                        + chunk.stream().map(id -> "?").collect(Collectors.joining(", ")) + ")";
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    for (int i = 0; i < chunk.size(); i++) {
                        ps.setString(i + 1, chunk.get(i));
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.put(rs.getString(1), rs.getLong(2));
                        }
                    }
                }
            }
        }
        return out;
    }

    @Override
    public synchronized Map<TopicPartition, Long> storedOffsets() {
        Map<TopicPartition, Long> out = new HashMap<>();
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT topic, \"partition\", next_offset FROM " + OFFSETS_TABLE)) {
            while (rs.next()) {
                out.put(new TopicPartition(rs.getString(1), rs.getInt(2)), rs.getLong(3));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("reading " + OFFSETS_TABLE + " failed: " + e.getMessage(), e);
        }
        return out;
    }

    private PreparedStatement prepare(String sql) throws SQLException {
        PreparedStatement ps = statements.get(sql);
        if (ps == null) {
            ps = conn.prepareStatement(sql);
            statements.put(sql, ps);
        }
        return ps;
    }

    /** The version-guarded upsert of a root row. */
    static String upsertSql(TableSpec root) {
        String cols = root.columns().stream().map(c -> q(c.name())).collect(Collectors.joining(", "));
        String vals = root.columns().stream().map(c -> placeholder(c.type())).collect(Collectors.joining(", "));
        String sets = root.columns().stream().filter(c -> c.source() != ColumnSpec.Source.ENTITY_ID)
                .map(c -> q(c.name()) + " = excluded." + q(c.name())).collect(Collectors.joining(", "));
        return "INSERT INTO " + q(root.name()) + " (" + cols + ") VALUES (" + vals + ") ON CONFLICT (entity_id) DO UPDATE SET "
                + sets + " WHERE excluded.entity_version > " + q(root.name()) + ".entity_version";
    }

    private static String placeholder(ColumnType type) {
        return switch (type) {
            case DECIMAL, DATE, TIMESTAMPTZ, JSON -> "CAST(? AS " + sqlType(type) + ")";
            default -> "?";
        };
    }

    private static void bind(PreparedStatement ps, List<ColumnSpec> columns, Object[] row) throws SQLException {
        for (int i = 0; i < row.length; i++) {
            Object v = row[i];
            int idx = i + 1;
            ColumnType type = columns.get(i).type();
            if (v == null) {
                ps.setNull(idx, switch (type) {
                    case BIGINT -> Types.BIGINT;
                    case DOUBLE -> Types.DOUBLE;
                    case BOOLEAN -> Types.BOOLEAN;
                    default -> Types.VARCHAR;
                });
                continue;
            }
            switch (type) {
                case BIGINT -> ps.setLong(idx, (Long) v);
                case DOUBLE -> ps.setDouble(idx, (Double) v);
                case BOOLEAN -> ps.setBoolean(idx, (Boolean) v);
                case DECIMAL -> ps.setString(idx, ((BigDecimal) v).toPlainString());
                case TIMESTAMPTZ -> ps.setString(idx, TIMESTAMP.format((OffsetDateTime) v));
                default -> ps.setString(idx, v.toString()); // strings, enums, JSON text, ISO dates
            }
        }
    }

    /** Quotes an identifier. */
    static String q(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    private void closeStatements() {
        for (PreparedStatement ps : statements.values()) {
            try {
                ps.close();
            } catch (SQLException e) {
                log.debug("closing a statement failed: {}", e.getMessage());
            }
        }
        statements.clear();
    }

    @Override
    public synchronized void close() {
        try {
            closeStatements();
            conn.close();
        } catch (SQLException e) {
            log.warn("closing DuckDB failed: {}", e.getMessage());
        }
    }
}
