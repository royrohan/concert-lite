package io.concert.sink;

/**
 * Database-neutral column types of a {@link SchemaSpec}; each target renders them (DuckDB:
 * {@code VARCHAR, BIGINT, DOUBLE, DECIMAL(38,4), BOOLEAN, DATE, TIMESTAMPTZ, VARCHAR, JSON}).
 */
public enum ColumnType {
    /** Pure {@code String}, and {@code Date} (which may be a date or a date-time). */
    STRING,
    /** Pure {@code Integer}. */
    BIGINT,
    /** Pure {@code Float} and {@code Number}. */
    DOUBLE,
    /** Pure {@code Decimal}; DECIMAL(38,4) by default. */
    DECIMAL,
    BOOLEAN,
    /** Pure {@code StrictDate}. */
    DATE,
    /** Pure {@code DateTime}, stored as an instant (UTC). */
    TIMESTAMPTZ,
    /** A Pure enum, stored as its value name. */
    ENUM,
    /** Anything not flattened (to-many values, deeper nesting, the whole model), as a JSON document. */
    JSON
}
