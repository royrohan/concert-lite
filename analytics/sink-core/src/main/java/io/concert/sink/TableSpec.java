package io.concert.sink;

import java.util.List;
import java.util.Optional;

/**
 * A table of a {@link SchemaSpec}.
 *
 * <ul>
 *   <li>{@link Kind#ROOT}: one row per entity of {@code smType}, primary key {@code entity_id};
 *   <li>{@link Kind#CHILD}: one row per element of the root model's owned to-many association
 *       {@code sourcePath}, keyed by {@code (entity_id, idx)} and replaced whenever the entity's
 *       version advances.
 * </ul>
 *
 * @param pureClass the qualified Pure class a row represents
 * @param parent the root table of a child table, else {@code null}
 * @param sourcePath for a child table, the JSON field path of the array in the root model
 */
public record TableSpec(String name, Kind kind, String smType, String pureClass, String parent, List<String> sourcePath,
        List<ColumnSpec> columns) {

    public enum Kind { ROOT, CHILD }

    public TableSpec {
        sourcePath = List.copyOf(sourcePath);
        columns = List.copyOf(columns);
    }

    public Optional<ColumnSpec> column(String columnName) {
        return columns.stream().filter(c -> c.name().equals(columnName)).findFirst();
    }

    /** Columns read from the model JSON. */
    public List<ColumnSpec> modelColumns() {
        return columns.stream().filter(c -> c.source() == ColumnSpec.Source.MODEL).toList();
    }
}
