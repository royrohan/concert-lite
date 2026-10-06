package io.concert.sink;

import java.util.List;

/**
 * One column of a {@link TableSpec}.
 *
 * @param source where the value comes from: a snapshot field, the element index of a child row, or
 *     {@link Source#MODEL} (a field of the model JSON)
 * @param path for {@link Source#MODEL}: the JSON field names from the row's object (the root model
 *     for root tables, the array element for child tables) to the value; empty otherwise
 * @param group for a column flattened out of a value class: the dotted property path of that value
 *     (e.g. {@code total} for {@code total_amount}), else {@code null}; renderers may use it to build
 *     a STRUCT/Tuple instead, sample queries to spot Money-like groups
 */
public record ColumnSpec(String name, ColumnType type, Source source, List<String> path, String group) {

    public enum Source { ENTITY_ID, ENTITY_VERSION, SM_TYPE, SM_STATE, CREATED_AT, COMPLETED_AT, MODEL_JSON, IDX, MODEL }

    public ColumnSpec {
        path = List.copyOf(path);
    }

    static ColumnSpec system(String name, ColumnType type, Source source) {
        return new ColumnSpec(name, type, source, List.of(), null);
    }

    /** The last element of {@link #path()}, or the column name for system columns. */
    public String leaf() {
        return path.isEmpty() ? name : path.getLast();
    }
}
