package io.concert.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts the rows of one snapshot for a root table and its child tables. Values are typed by
 * {@link ColumnType}: STRING/ENUM {@code String}, BIGINT {@code Long}, DOUBLE {@code Double},
 * DECIMAL {@code BigDecimal}, BOOLEAN {@code Boolean}, DATE {@code LocalDate}, TIMESTAMPTZ
 * {@code OffsetDateTime} (UTC), JSON {@code String} (JSON text). Missing or unparsable values are
 * {@code null}: the snapshot already passed the model's validation, so this only happens when the
 * model changed incompatibly, and the full document is still in {@code model_json}.
 */
public final class SnapshotRows {

    private static final ObjectMapper MAPPER = SnapshotCodec.MAPPER;

    /** @param children child table to its rows, in {@link SchemaSpec#childrenOf} order */
    public record EntityRows(TableSpec root, Object[] rootRow, Map<TableSpec, List<Object[]>> children) {}

    private SnapshotRows() {}

    public static EntityRows extract(SchemaSpec schema, TableSpec root, EntitySnapshot s) {
        JsonNode model;
        try {
            model = s.modelJson() == null ? MissingNode.getInstance() : MAPPER.readTree(s.modelJson());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("snapshot of " + s.entityId() + " has invalid model JSON", e);
        }
        Object[] rootRow = row(root, s, model, -1);
        Map<TableSpec, List<Object[]>> children = new LinkedHashMap<>();
        for (TableSpec child : schema.childrenOf(root)) {
            JsonNode array = at(model, child.sourcePath());
            List<Object[]> rows = new ArrayList<>();
            if (array.isArray()) {
                for (int i = 0; i < array.size(); i++) {
                    rows.add(row(child, s, array.get(i), i));
                }
            }
            children.put(child, rows);
        }
        return new EntityRows(root, rootRow, children);
    }

    private static Object[] row(TableSpec table, EntitySnapshot s, JsonNode object, int idx) {
        Object[] row = new Object[table.columns().size()];
        for (int i = 0; i < row.length; i++) {
            ColumnSpec c = table.columns().get(i);
            row[i] = switch (c.source()) {
                case ENTITY_ID -> s.entityId();
                case ENTITY_VERSION -> s.version();
                case SM_TYPE -> s.smType();
                case SM_STATE -> s.state();
                case CREATED_AT -> utc(s.createdAt());
                case COMPLETED_AT -> utc(s.completedAt());
                case MODEL_JSON -> s.modelJson();
                case IDX -> (long) idx;
                case MODEL -> value(c.type(), at(object, c.path()));
            };
        }
        return row;
    }

    private static JsonNode at(JsonNode node, List<String> path) {
        JsonNode n = node;
        for (String field : path) {
            n = n.path(field);
        }
        return n;
    }

    private static Object value(ColumnType type, JsonNode v) {
        if (v.isMissingNode() || v.isNull()) {
            return null;
        }
        try {
            return switch (type) {
                case STRING, ENUM -> v.isValueNode() ? v.asText() : v.toString();
                case BIGINT -> v.canConvertToLong() ? v.asLong() : Long.valueOf(v.asText());
                case DOUBLE -> v.isNumber() ? v.asDouble() : Double.valueOf(v.asText());
                case DECIMAL -> v.isNumber() ? v.decimalValue() : new BigDecimal(v.asText());
                case BOOLEAN -> v.isBoolean() ? v.asBoolean() : Boolean.valueOf(v.asText());
                case DATE -> LocalDate.parse(v.asText());
                case TIMESTAMPTZ -> OffsetDateTime.parse(v.asText()).withOffsetSameInstant(ZoneOffset.UTC);
                case JSON -> v.toString();
            };
        } catch (NumberFormatException | DateTimeParseException e) {
            return null;
        }
    }

    private static OffsetDateTime utc(Instant t) {
        return t == null ? null : t.atOffset(ZoneOffset.UTC);
    }
}
