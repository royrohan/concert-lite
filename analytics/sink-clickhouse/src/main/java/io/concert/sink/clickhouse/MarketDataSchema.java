package io.concert.sink.clickhouse;

import io.concert.model.pure.ClassDef;
import io.concert.model.pure.PropertyDef;
import io.concert.model.pure.ResolvedModel;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ClickHouse objects for the market-data side path ({@code marketdata-sim} writes straight to Kafka,
 * bypassing concert), <b>generated from the trading Pure models</b> ({@code core/trading-model/src/main/pure}:
 * {@code trading::refdata::Instrument|Account|Venue}, {@code trading::marketdata::Tick|Bar}). Same
 * pattern per topic: a Kafka queue, a materialized view, a MergeTree-family table.
 *
 * <pre>
 * ref.instruments ─▶ instruments  ReplacingMergeTree(kafka_offset) ORDER BY symbol
 * ref.accounts    ─▶ accounts     ReplacingMergeTree(kafka_offset) ORDER BY account_id
 * ref.venues      ─▶ venues       ReplacingMergeTree(kafka_offset) ORDER BY mic
 * md.ticks        ─▶ ticks        MergeTree ORDER BY (symbol, ts), TTL 1 day
 * md.bars.1m      ─▶ bars_1m      MergeTree ORDER BY (symbol, start), TTL 7 days
 * </pre>
 *
 * Values are {@code ModelJson}: absent optional fields are omitted, so required properties are plain
 * queue columns (a missing one reads as the type's default) and optional ones {@code Nullable}.
 * Timestamps are read as strings and parsed in the view. Reference tables are versioned by the Kafka
 * offset (single-partition compacted topics), since {@code updatedAt} is optional in the model. Table
 * columns are the properties in snake case; {@code Decimal} is {@code Decimal(38, 6)}, enums
 * {@code LowCardinality(String)}. Consumer groups are {@code sink-clickhouse-<topic, dots as dashes>}.
 */
public final class MarketDataSchema {

    public static final String GROUP_PREFIX = ClickHouseSchema.DEFAULT_GROUP + "-";

    /** One Kafka topic and the Pure class of its values. */
    public record Source(String topic, String pureClass, String table, boolean reference, String engineTail) {}

    public static final List<Source> SOURCES = List.of(
            new Source("ref.instruments", "trading::refdata::Instrument", "instruments", true, "ORDER BY symbol"),
            new Source("ref.accounts", "trading::refdata::Account", "accounts", true, "ORDER BY account_id"),
            new Source("ref.venues", "trading::refdata::Venue", "venues", true, "ORDER BY mic"),
            new Source("md.ticks", "trading::marketdata::Tick", "ticks", false,
                    "PARTITION BY toDate(ts)\nORDER BY (symbol, ts)\nTTL toDateTime(ts) + INTERVAL 1 DAY"),
            new Source("md.bars.1m", "trading::marketdata::Bar", "bars_1m", false,
                    "PARTITION BY toDate(start)\nORDER BY (symbol, start)\nTTL toDateTime(start) + INTERVAL 7 DAY"));

    private MarketDataSchema() {}

    public static List<Ddl> ddl(ResolvedModel model, String brokers) {
        List<Ddl> out = new ArrayList<>();
        for (Source s : SOURCES) {
            ClassDef cls = model.findClass(s.pureClass()).orElseThrow(() -> new IllegalArgumentException(
                    "class " + s.pureClass() + " (topic " + s.topic() + ") is not in the trading models"));
            List<PropertyDef> props = model.allProperties(cls).stream().filter(p -> p.multiplicity().isToOne()).toList();
            String queue = s.topic().replace('.', '_') + "_queue";

            List<Ddl.Column> cols = new ArrayList<>();
            List<String> select = new ArrayList<>();
            List<String> queueCols = new ArrayList<>();
            for (PropertyDef p : props) {
                String col = ClickHouseSchema.snake(p.name());
                boolean optional = p.multiplicity().isOptional();
                String base = baseType(model, p);
                cols.add(new Ddl.Column(col, optional ? nullable(base) : base));
                boolean dateTime = isDateTime(p);
                String queueType = dateTime ? "String" : model.isEnum(p.type()) ? "String" : base;
                queueCols.add(Sql.quoted(p.name()) + " " + (optional ? "Nullable(" + queueType + ")" : queueType));
                String src = Sql.quoted(p.name());
                String expr = dateTime
                        ? "parseDateTime64BestEffort" + (optional ? "OrNull" : "") + "(" + src + ", 3, 'UTC')"
                        : src;
                select.add(expr + " AS " + Sql.quoted(col));
            }
            if (s.reference()) {
                cols.add(new Ddl.Column("kafka_offset", "UInt64"));
                select.add("_offset AS kafka_offset");
            }
            out.add(new Ddl.Table(s.table(), cols,
                    (s.reference() ? "ENGINE = ReplacingMergeTree(kafka_offset)\n" : "ENGINE = MergeTree\n") + s.engineTail(),
                    "marketdata " + s.topic() + " " + s.pureClass()));
            out.add(new Ddl.Queue(queue, "CREATE TABLE " + queue + "\n(\n    " + String.join(",\n    ", queueCols) + "\n)\n"
                    + ClickHouseSchema.kafkaEngine(brokers, s.topic(), GROUP_PREFIX + s.topic().replace('.', '-'))));
            out.add(new Ddl.MaterializedView("mv_" + s.table(), queue, s.table(),
                    "SELECT\n    " + select.stream().collect(Collectors.joining(",\n    ")) + "\nFROM " + queue
                            + "\nWHERE length(_error) = 0", null));
            out.add(ClickHouseSchema.errorView(queue));
        }
        return out;
    }

    private static boolean isDateTime(PropertyDef p) {
        return p.type().primitive() && switch (p.type().primitiveType()) {
            case DATE_TIME -> true;
            default -> false;
        };
    }

    /** Non-nullable ClickHouse type of a to-one property. */
    static String baseType(ResolvedModel model, PropertyDef p) {
        if (model.isEnum(p.type())) {
            return "LowCardinality(String)";
        }
        if (!p.type().primitive()) {
            return "String"; // a nested class: kept as raw JSON (none in the market-data models today)
        }
        return switch (p.type().primitiveType()) {
            case STRING, DATE -> "String";
            case INTEGER -> "Int64";
            case FLOAT, NUMBER -> "Float64";
            case DECIMAL -> "Decimal(38, 6)";
            case BOOLEAN -> "Bool";
            case STRICT_DATE -> "Date32";
            case DATE_TIME -> "DateTime64(3, 'UTC')";
        };
    }

    private static String nullable(String base) {
        return base.startsWith("LowCardinality(") ? "LowCardinality(Nullable(" + base.substring(15) + ")" : "Nullable(" + base + ")";
    }
}
