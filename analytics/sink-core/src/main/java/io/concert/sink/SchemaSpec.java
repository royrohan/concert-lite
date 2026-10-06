package io.concert.sink;

import java.util.List;
import java.util.Optional;

/**
 * The tables a sink keeps for completed entities, derived from the Pure models by
 * {@link SchemaMapper} and rendered to DDL by each {@link SinkTarget}. Root tables come first, each
 * followed by its child tables.
 */
public record SchemaSpec(List<TableSpec> tables) {

    public SchemaSpec {
        tables = List.copyOf(tables);
    }

    public List<TableSpec> roots() {
        return tables.stream().filter(t -> t.kind() == TableSpec.Kind.ROOT).toList();
    }

    public Optional<TableSpec> rootFor(String smType) {
        return tables.stream().filter(t -> t.kind() == TableSpec.Kind.ROOT && t.smType().equals(smType)).findFirst();
    }

    public List<TableSpec> childrenOf(TableSpec root) {
        return tables.stream().filter(t -> root.name().equals(t.parent())).toList();
    }

    public Optional<TableSpec> table(String name) {
        return tables.stream().filter(t -> t.name().equals(name)).findFirst();
    }
}
