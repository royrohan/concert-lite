package io.concert.sink.clickhouse;

import io.concert.sink.ColumnSpec;
import io.concert.sink.ColumnType;
import io.concert.sink.SchemaSpec;
import io.concert.sink.SnapshotCodec;
import io.concert.sink.TableSpec;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The ClickHouse objects for completed entities, generated from a {@link SchemaSpec}:
 *
 * <pre>
 * entity-snapshots (Kafka) ─▶ entity_snapshots_queue  (ENGINE = Kafka, group sink-clickhouse, JSONEachRow,
 *                                 │                     model read as a raw JSON string, error mode 'stream')
 *                                 ├─ mv_entity_snapshots ─▶ entity_snapshots  ReplacingMergeTree(entity_version)
 *                                 │                            ├─ mv_orders    WHERE sm_type = 'order'    ─▶ orders
 *                                 │                            ├─ mv_payments  WHERE sm_type = 'payment'  ─▶ payments
 *                                 │                            └─ ...
 *                                 └─ mv_entity_snapshots_queue_errors ─▶ kafka_errors (_error, _raw_message)
 * order_lines = VIEW: orders FINAL ARRAY JOIN lines
 * </pre>
 *
 * <ul>
 *   <li>Root tables ({@code orders}, ...) have the same column names as sink-duckdb: the system columns
 *       and the model's to-one properties, value classes <b>flattened</b> ({@code total_amount},
 *       {@code total_currency}) so queries port between the two stores. Enums are
 *       {@code LowCardinality(Nullable(String))}, {@code Decimal} is {@code Decimal(38, 4)}, {@code DateTime}
 *       is {@code DateTime64(3, 'UTC')}; optional model values are {@code Nullable}. Engine
 *       {@code ReplacingMergeTree(entity_version) ORDER BY entity_id}: read with {@code FINAL} (or
 *       {@code argMax}) for exactly one row per entity, the highest version.
 *   <li>Owned to-many children are an {@code Array(Tuple(idx UInt32, <flattened columns>))} column named
 *       after the association ({@code lines}), plus a view named like the DuckDB child table
 *       ({@code order_lines}: {@code entity_id, entity_version, idx, ...}) that ARRAY JOINs it against
 *       {@code <root> FINAL}.
 *   <li>{@code entity_snapshots} keeps every snapshot's raw model (latest version per entity), so a new or
 *       changed per-type view can be backfilled with {@code INSERT ... SELECT ... FROM entity_snapshots FINAL}
 *       instead of rewinding Kafka.
 * </ul>
 */
public final class ClickHouseSchema {

    public static final String QUEUE = "entity_snapshots_queue";
    public static final String RAW = "entity_snapshots";
    public static final String ERRORS = "kafka_errors";
    public static final String SAMPLES = "_concert_samples";
    public static final String DEFAULT_GROUP = "sink-clickhouse";

    /** @param brokers the broker list as ClickHouse reaches it ({@code kafka:9092} in compose) */
    public record Config(String brokers, String topic, String group) {
        public static Config defaults(String brokers) {
            return new Config(brokers, SnapshotCodec.TOPIC, DEFAULT_GROUP);
        }
    }

    private static final String DT = "DateTime64(3, 'UTC')";
    private static final String ARRAY_PREFIX_GUARD = "_items";

    private ClickHouseSchema() {}

    /** Objects shared by all sources: the error table and the sample-query table. */
    public static List<Ddl> common() {
        return List.of(
                new Ddl.Table(ERRORS, List.of(
                        new Ddl.Column("at", DT),
                        new Ddl.Column("source", "LowCardinality(String)"),
                        new Ddl.Column("topic", "LowCardinality(String)"),
                        new Ddl.Column("kafka_partition", "UInt64"),
                        new Ddl.Column("kafka_offset", "UInt64"),
                        new Ddl.Column("error", "String"),
                        new Ddl.Column("raw_message", "String")),
                        "ENGINE = MergeTree\nORDER BY at\nTTL toDateTime(at) + INTERVAL 30 DAY",
                        "concert errors: records a Kafka queue could not parse"),
                new Ddl.Table(SAMPLES, List.of(
                        new Ddl.Column("ord", "UInt32"),
                        new Ddl.Column("title", "String"),
                        new Ddl.Column("sql", "String")),
                        "ENGINE = MergeTree\nORDER BY ord",
                        "concert samples: example queries for the trace UI, rewritten by the provisioner"));
    }

    /** The Kafka queue, the raw snapshot table and the per-root tables, views and materialized views. */
    public static List<Ddl> entities(SchemaSpec schema, Config cfg) {
        List<Ddl> out = new ArrayList<>();
        out.add(rawTable());
        for (TableSpec root : schema.roots()) {
            out.add(rootTable(schema, root));
        }
        for (TableSpec root : schema.roots()) {
            for (TableSpec child : schema.childrenOf(root)) {
                out.add(childView(schema, root, child));
            }
        }
        for (TableSpec root : schema.roots()) {
            out.add(new Ddl.MaterializedView("mv_" + root.name(), RAW, root.name(),
                    rootSelect(schema, root, RAW + "\nWHERE sm_type = " + Sql.str(root.smType())),
                    insert(rootTable(schema, root), rootSelect(schema, root,
                            RAW + " FINAL\nWHERE sm_type = " + Sql.str(root.smType())))));
        }
        out.add(queue(cfg));
        out.add(new Ddl.MaterializedView("mv_" + RAW, QUEUE, RAW, rawSelect(), null));
        out.add(errorView(QUEUE));
        return out;
    }

    // ------------------------------------------------------------------ queue and raw table

    static Ddl.Queue queue(Config cfg) {
        return new Ddl.Queue(QUEUE, "CREATE TABLE " + QUEUE + "\n(\n"
                + "    entityId String,\n    smType String,\n    state Nullable(String),\n    version UInt64,\n"
                + "    createdAt Nullable(String),\n    completedAt Nullable(String),\n"
                + "    model Nullable(String)\n)\n"
                + kafkaEngine(cfg.brokers(), cfg.topic(), cfg.group()));
    }

    /**
     * Kafka engine settings shared by every queue. {@code input_format_json_read_objects_as_strings}
     * delivers a nested object into a {@code String} column as its raw JSON text; error mode
     * {@code stream} fills the {@code _error}/{@code _raw_message} virtual columns instead of stopping
     * the consumer on a bad record.
     */
    static String kafkaEngine(String brokers, String topic, String group) {
        return "ENGINE = Kafka\nSETTINGS kafka_broker_list = " + Sql.str(brokers) + ", kafka_topic_list = " + Sql.str(topic)
                + ", kafka_group_name = " + Sql.str(group) + ",\n    kafka_format = 'JSONEachRow', kafka_handle_error_mode = 'stream',"
                + " kafka_num_consumers = 1,\n    input_format_json_read_objects_as_strings = 1, input_format_skip_unknown_fields = 1";
    }

    static Ddl.Table rawTable() {
        return new Ddl.Table(RAW, List.of(
                new Ddl.Column("entity_id", "String"),
                new Ddl.Column("entity_version", "UInt64"),
                new Ddl.Column("sm_type", "LowCardinality(String)"),
                new Ddl.Column("sm_state", "LowCardinality(String)"),
                new Ddl.Column("created_at", "Nullable(" + DT + ")"),
                new Ddl.Column("completed_at", "Nullable(" + DT + ")"),
                new Ddl.Column("model_json", "String"),
                new Ddl.Column("kafka_partition", "UInt32"),
                new Ddl.Column("kafka_offset", "UInt64"),
                new Ddl.Column("ingested_at", DT, "now64(3)")),
                "ENGINE = ReplacingMergeTree(entity_version)\nORDER BY entity_id",
                "concert snapshots: latest snapshot per entity, model as raw JSON");
    }

    private static String rawSelect() {
        return "SELECT entityId AS entity_id, version AS entity_version, smType AS sm_type, ifNull(state, '') AS sm_state,\n"
                + "    parseDateTime64BestEffortOrNull(createdAt, 3, 'UTC') AS created_at,\n"
                + "    parseDateTime64BestEffortOrNull(completedAt, 3, 'UTC') AS completed_at,\n"
                + "    ifNull(model, '') AS model_json, _partition AS kafka_partition, _offset AS kafka_offset,"
                + " now64(3) AS ingested_at\n"
                + "FROM " + QUEUE + "\nWHERE length(_error) = 0 AND entityId != ''";
    }

    /** The error view of a queue: unparsable records into {@link #ERRORS}. */
    static Ddl.MaterializedView errorView(String queue) {
        return new Ddl.MaterializedView("mv_" + queue + "_errors", queue, ERRORS,
                "SELECT now64(3) AS at, " + Sql.str(queue) + " AS source, _topic AS topic, _partition AS kafka_partition,"
                        + " _offset AS kafka_offset,\n    _error AS error, _raw_message AS raw_message\n"
                        + "FROM " + queue + "\nWHERE length(_error) > 0", null);
    }

    // ------------------------------------------------------------------ typed tables

    static Ddl.Table rootTable(SchemaSpec schema, TableSpec root) {
        List<Ddl.Column> cols = new ArrayList<>();
        for (ColumnSpec c : root.columns()) {
            cols.add(new Ddl.Column(c.name(), switch (c.source()) {
                case ENTITY_ID -> "String";
                case ENTITY_VERSION -> "UInt64";
                case SM_TYPE, SM_STATE -> "LowCardinality(String)";
                case CREATED_AT, COMPLETED_AT -> "Nullable(" + DT + ")";
                case MODEL_JSON -> "String";
                case IDX -> "UInt32";
                case MODEL -> type(c.type());
            }));
        }
        for (TableSpec child : schema.childrenOf(root)) {
            cols.add(new Ddl.Column(arrayColumn(root, child), tupleType(child)));
        }
        return new Ddl.Table(root.name(), cols, "ENGINE = ReplacingMergeTree(entity_version)\nORDER BY entity_id",
                "concert root " + root.smType());
    }

    /** The Array(Tuple) column of a child table: the association name, de-duplicated against root columns. */
    static String arrayColumn(TableSpec root, TableSpec child) {
        String name = snake(child.sourcePath().getLast());
        Set<String> taken = new HashSet<>();
        root.columns().forEach(c -> taken.add(c.name()));
        return taken.contains(name) ? name + ARRAY_PREFIX_GUARD : name;
    }

    static String tupleType(TableSpec child) {
        return "Array(Tuple(" + childElements(child).stream().map(c -> Sql.id(c.name()) + " "
                + (c.source() == ColumnSpec.Source.IDX ? "UInt32" : type(c.type()))).collect(Collectors.joining(", ")) + "))";
    }

    /** The child columns stored in the tuple: {@code idx} and the model columns (the entity keys come from the root). */
    private static List<ColumnSpec> childElements(TableSpec child) {
        return child.columns().stream()
                .filter(c -> c.source() == ColumnSpec.Source.IDX || c.source() == ColumnSpec.Source.MODEL).toList();
    }

    /** ClickHouse type of a model column. */
    static String type(ColumnType t) {
        return switch (t) {
            case STRING -> "Nullable(String)";
            case ENUM -> "LowCardinality(Nullable(String))";
            case BIGINT -> "Nullable(Int64)";
            case DOUBLE -> "Nullable(Float64)";
            case DECIMAL -> "Nullable(Decimal(38, 4))";
            case BOOLEAN -> "Nullable(Bool)";
            case DATE -> "Nullable(Date32)";
            case TIMESTAMPTZ -> "Nullable(" + DT + ")";
            case JSON -> "Nullable(String)";
        };
    }

    /**
     * The expression reading {@code c} (a path below the JSON object {@code src}). Every conversion must be
     * total (NULL on bad input, never an exception), see the DECIMAL case.
     */
    static String extract(String src, ColumnSpec c) {
        String keys = c.path().stream().map(Sql::str).collect(Collectors.joining(", "));
        String args = src + ", " + keys;
        return switch (c.type()) {
            case STRING, ENUM -> "JSONExtract(" + args + ", 'Nullable(String)')";
            case BIGINT -> "JSONExtract(" + args + ", 'Nullable(Int64)')";
            case DOUBLE -> "JSONExtract(" + args + ", 'Nullable(Float64)')";
            // Not JSONExtract(..., 'Decimal'): it throws on overflow (e.g. 1e40), and an exception in a
            // materialized view makes the Kafka engine retry the block forever (kafka_handle_error_mode only
            // covers parse errors). The OrNull conversion of the raw text (quotes trimmed for string-encoded
            // decimals) gives NULL instead, like the DuckDB sink's lenient conversions.
            case DECIMAL -> "toDecimal128OrNull(trim(BOTH '\"' FROM JSONExtractRaw(" + args + ")), 4)";
            case BOOLEAN -> "JSONExtract(" + args + ", 'Nullable(Bool)')";
            case DATE -> "toDate32OrNull(JSONExtractString(" + args + "))";
            case TIMESTAMPTZ -> "parseDateTime64BestEffortOrNull(JSONExtractString(" + args + "), 3, 'UTC')";
            case JSON -> "nullIf(JSONExtractRaw(" + args + "), '')";
        };
    }

    /** The per-type SELECT over {@code from} (the raw table plus filter). */
    static String rootSelect(SchemaSpec schema, TableSpec root, String from) {
        List<String> items = new ArrayList<>();
        for (ColumnSpec c : root.columns()) {
            items.add(c.source() == ColumnSpec.Source.MODEL ? extract("model_json", c) + " AS " + Sql.id(c.name()) : Sql.id(c.name()));
        }
        for (TableSpec child : schema.childrenOf(root)) {
            String arr = "JSONExtractArrayRaw(model_json, " + child.sourcePath().stream().map(Sql::str)
                    .collect(Collectors.joining(", ")) + ")";
            String elements = childElements(child).stream()
                    .map(c -> c.source() == ColumnSpec.Source.IDX ? "toUInt32(i)" : extract("e", c))
                    .collect(Collectors.joining(",\n            "));
            items.add("CAST(arrayMap((e, i) -> tuple(\n            " + elements + "),\n        " + arr + ", range(length(" + arr
                    + "))), " + Sql.str(tupleType(child)) + ") AS " + Sql.id(arrayColumn(root, child)));
        }
        return "SELECT\n    " + String.join(",\n    ", items) + "\nFROM " + from;
    }

    private static String insert(Ddl.Table target, String select) {
        return "INSERT INTO " + Sql.id(target.name()) + " ("
                + target.columns().stream().map(c -> Sql.id(c.name())).collect(Collectors.joining(", ")) + ")\n" + select;
    }

    static Ddl.View childView(SchemaSpec schema, TableSpec root, TableSpec child) {
        List<String> items = new ArrayList<>(List.of("entity_id", "entity_version"));
        for (ColumnSpec c : childElements(child)) {
            items.add("c." + Sql.id(c.name()) + " AS " + Sql.id(c.name()));
        }
        return new Ddl.View(child.name(), "SELECT " + String.join(", ", items) + "\nFROM " + Sql.id(root.name())
                + " FINAL\nARRAY JOIN " + Sql.id(arrayColumn(root, child)) + " AS c",
                "concert child " + root.smType() + " " + root.name());
    }

    /** Same rule as SchemaMapper's (package-private there). */
    static String snake(String camel) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camel.length(); i++) {
            char c = camel.charAt(i);
            if (Character.isUpperCase(c)) {
                boolean prevLower = i > 0 && !Character.isUpperCase(camel.charAt(i - 1)) && camel.charAt(i - 1) != '_';
                boolean nextLower = i + 1 < camel.length() && Character.isLowerCase(camel.charAt(i + 1));
                if (i > 0 && (prevLower || (nextLower && Character.isUpperCase(camel.charAt(i - 1))))) {
                    sb.append('_');
                }
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c == '-' ? '_' : c);
            }
        }
        return sb.toString();
    }
}
