package io.concert.legend;

import io.concert.model.pure.AssociationEnd;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.EnumDef;
import io.concert.model.pure.PropertyDef;
import io.concert.model.pure.ResolvedModel;
import io.concert.sink.ColumnSpec;
import io.concert.sink.ColumnType;
import io.concert.sink.SchemaSpec;
import io.concert.sink.TableSpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The Legend store layer of the analytics sinks, as Pure source for legend-lite: from the same {@link SchemaSpec} the
 * sinks build their tables from (so the two can't drift), one store layer for both DuckDB and ClickHouse:
 *
 * <ul>
 *   <li>a {@code ###Relational} Database with every table and column the sink creates (system columns included, so
 *       {@code #>{db.table}#} reaches {@code entity_version}, {@code sm_state}, {@code created_at}, ...), the root's
 *       primary key {@code entity_id}, and one Join per child table on {@code entity_id};
 *   <li>a {@code ###Mapping}: one Relational class mapping per table (root class onto the root table, element class
 *       onto the child table), value classes and owned to-one associations as embedded mappings (the sinks flatten
 *       them into prefixed columns), an EnumerationMapping per enum used (values stored by name), and an
 *       AssociationMapping per child table. System columns and JSON columns (to-many values, deep nesting) are not
 *       mapped to properties;
 *   <li>per store a {@code ###Connection} and a {@code ###Runtime} over that same Database and Mapping: the runtime
 *       alone picks the dialect, so one query (class-based or table access {@code #>{concert::store::Sink.orders_latest}#})
 *       plans for either store. Execution stays with concert (legend-lite only plans SQL), so connection details are
 *       nominal.
 * </ul>
 *
 * Roots are read through their {@code <root>_latest} view in both stores, exactly one row per entity: in ClickHouse the
 * ReplacingMergeTree table {@code FINAL} (sink-clickhouse creates it), in DuckDB the table itself (sink-duckdb creates
 * the view under the same name). Child tables have the same name in both (in ClickHouse, views over
 * {@code <root> FINAL}).
 *
 * <p>Event-style roots ({@code evt_*}) map the payload class onto the columns below {@code payload}; the lifecycle
 * columns (status, attempts, ...) are reachable by table access only.
 */
public final class LegendStoreGenerator {

    /** The one Database and Mapping of both stores. */
    public static final String DATABASE = "concert::store::Sink";
    public static final String MAPPING = "concert::mapping::Sink";

    /** A store a runtime targets: element-name suffix and legend-lite database type. */
    public enum Target {
        DUCKDB("DuckDb", "DuckDB"),
        CLICKHOUSE("ClickHouse", "ClickHouse");

        final String id;
        final String dbType;

        Target(String id, String dbType) {
            this.id = id;
            this.dbType = dbType;
        }

        public String runtime() {
            return "concert::runtime::" + id;
        }

        public String connection() {
            return "concert::connection::" + id;
        }

        public static Target parse(String s) {
            return switch (s.trim().toLowerCase(Locale.ROOT)) {
                case "duckdb" -> DUCKDB;
                case "clickhouse" -> CLICKHOUSE;
                default -> throw new IllegalArgumentException("unknown target " + s + ": duckdb or clickhouse");
            };
        }
    }

    /** Suffix of a root's one-row-per-entity view, in both stores. */
    public static final String LATEST = "_latest";

    private final ResolvedModel model;
    private final SchemaSpec schema;

    public LegendStoreGenerator(ResolvedModel model, SchemaSpec schema) {
        this.model = model;
        this.schema = schema;
    }

    /** The table name in the store layer: roots through their {@code _latest} view. */
    public static String tableName(TableSpec t) {
        return t.kind() == TableSpec.Kind.ROOT ? t.name() + LATEST : t.name();
    }

    /** The whole store layer: the Database, the Mapping, and a connection and runtime per target. */
    public String generate(List<Target> targets) {
        StringBuilder sb = new StringBuilder();
        sb.append("// GENERATED from the sink schema (analytics/legend-model LegendStoreGenerator) -- do not edit.\n");
        elements(targets).values().forEach(code -> sb.append('\n').append(code));
        return sb.toString();
    }

    /**
     * The store layer one element at a time, each with its section header (qualified name to Pure text): the Database,
     * the Mapping, then a connection and a runtime per target. Legend Studio keeps one element per file.
     */
    public Map<String, String> elements(List<Target> targets) {
        Map<String, String> out = new LinkedHashMap<>();
        StringBuilder db = new StringBuilder();
        database(db);
        out.put(DATABASE, db.toString().stripLeading());
        StringBuilder m = new StringBuilder();
        mapping(m);
        out.put(MAPPING, m.toString().stripLeading());
        for (TableSpec v : variants()) {
            StringBuilder vm = new StringBuilder();
            List<TableSpec> tables = new ArrayList<>(List.of(v));
            tables.addAll(schema.childrenOf(v));
            mapping(vm, variantMapping(v), tables);
            out.put(variantMapping(v), vm.toString().stripLeading());
        }
        for (Target t : targets) {
            out.put(t.connection(), "###Connection\nRelationalDatabaseConnection " + t.connection() + "\n{\n  store: " + DATABASE
                    + ";\n  type: " + t.dbType + ";\n  specification: "
                    + (t == Target.DUCKDB ? "DuckDB { }" : "Static { name: 'default'; host: 'clickhouse'; port: 8123; }")
                    + ";\n  auth: Test;\n}\n");
        }
        for (Target t : targets) {
            out.put(t.runtime(), runtime(t.runtime(), MAPPING, t));
            for (TableSpec v : variants()) {
                out.put(variantRuntime(t, v.name()), runtime(variantRuntime(t, v.name()), variantMapping(v), t));
            }
        }
        return out;
    }

    private static String runtime(String name, String mapping, Target t) {
        return "###Runtime\nRuntime " + name + "\n{\n  mappings: [" + mapping + "];\n  connections: [ " + DATABASE + ": [ "
                + t.id.toLowerCase(Locale.ROOT) + ": " + t.connection() + " ] ];\n}\n";
    }

    /**
     * Root tables whose class an earlier root already maps (gen_trading_orders and trading_orders are both
     * trading::order::Order): in the default Mapping the class's root set is the earlier table, so {@code Order.all()}
     * reads that one. Each such table gets its own Mapping and runtimes, where it is the class's root set.
     */
    public List<TableSpec> variants() {
        Set<String> seen = new LinkedHashSet<>();
        List<TableSpec> out = new ArrayList<>();
        for (TableSpec t : schema.roots()) {
            if (!seen.add(t.pureClass())) {
                out.add(t);
            }
        }
        return out;
    }

    /** A variant's Mapping: {@code concert::mapping::Sink__<table>}. */
    public static String variantMapping(TableSpec t) {
        return MAPPING + "__" + t.name();
    }

    /** The runtime of {@code target} that reads a root table's class from that table: the default, or its variant's. */
    public static String variantRuntime(Target target, String table) {
        return table == null ? target.runtime() : target.runtime() + "__" + table;
    }

    // ---------------------------------------------------------------- ###Relational

    private void database(StringBuilder sb) {
        sb.append("\n###Relational\nDatabase ").append(DATABASE).append("\n(\n");
        for (TableSpec t : schema.tables()) {
            sb.append("  Table ").append(tableName(t)).append("\n  (\n");
            List<ColumnSpec> cols = t.columns();
            for (int i = 0; i < cols.size(); i++) {
                ColumnSpec c = cols.get(i);
                boolean pk = c.name().equals("entity_id") || (t.kind() == TableSpec.Kind.CHILD && c.name().equals("idx"));
                sb.append("    ").append(column(c.name())).append(' ').append(sqlType(c.type())).append(pk ? " PRIMARY KEY" : "")
                        .append(i + 1 < cols.size() ? ",\n" : "\n");
            }
            sb.append("  )\n");
        }
        for (TableSpec root : schema.roots()) {
            for (TableSpec child : schema.childrenOf(root)) {
                sb.append("  Join ").append(join(root, child)).append('(').append(tableName(root)).append(".entity_id = ")
                        .append(tableName(child)).append(".entity_id)\n");
            }
        }
        sb.append(")\n");
    }

    static String join(TableSpec root, TableSpec child) {
        return root.name() + "_" + child.name();
    }

    static String sqlType(ColumnType t) {
        return switch (t) {
            case STRING -> "VARCHAR(4096)";
            case ENUM -> "VARCHAR(256)";
            case JSON -> "VARCHAR(1048576)";
            case BIGINT -> "BIGINT";
            case DOUBLE -> "DOUBLE";
            case DECIMAL -> "DECIMAL(38, 4)";
            case BOOLEAN -> "BIT";
            case DATE -> "DATE";
            case TIMESTAMPTZ -> "TIMESTAMP";
        };
    }

    /** Column names that are Pure / relational keywords are quoted. */
    static String column(String name) {
        return KEYWORDS.contains(name) ? "\"" + name + "\"" : name;
    }

    private static final Set<String> KEYWORDS = Set.of("order", "group", "table", "join", "select", "from", "where", "schema",
            "view", "filter", "primary", "key", "distinct", "by", "and", "or", "not", "in", "is", "null", "true", "false");

    // ---------------------------------------------------------------- ###Mapping

    /** A property-tree node of one table: leaves are columns, inner nodes embedded (flattened) values. */
    private static final class Node {
        final Map<String, Node> children = new LinkedHashMap<>();
        ColumnSpec column;
        EnumDef enumType;

        boolean empty() {
            return column == null && children.values().stream().allMatch(Node::empty);
        }
    }

    private void mapping(StringBuilder sb) {
        mapping(sb, MAPPING, schema.tables());
    }

    /**
     * A Mapping over {@code tables} (in order; the first table of a class is its root set). The default Mapping covers
     * every table; a variant covers one root table, whose class another root already maps, plus its children.
     */
    private void mapping(StringBuilder sb, String mappingName, List<TableSpec> tables) {
        Map<String, EnumDef> enums = new LinkedHashMap<>();
        Map<TableSpec, Node> trees = new LinkedHashMap<>();
        for (TableSpec t : tables) {
            trees.put(t, tree(t, enums));
        }
        String db = "[" + DATABASE + "] ";
        sb.append("\n###Mapping\nMapping ").append(mappingName).append("\n(\n");
        for (EnumDef e : enums.values()) {
            sb.append("  ").append(e.qualifiedName()).append(": EnumerationMapping ").append(enumId(e)).append("\n  {\n");
            List<String> vs = e.values().stream().map(v -> "    " + v.name() + ": '" + v.name() + "'").toList();
            sb.append(String.join(",\n", vs)).append("\n  }\n");
        }
        Set<String> rootMapped = new LinkedHashSet<>();
        for (var entry : trees.entrySet()) {
            TableSpec t = entry.getKey();
            String table = tableName(t);
            boolean root = rootMapped.add(t.pureClass());
            sb.append("  ").append(root ? "*" : "").append(t.pureClass()).append('[').append(setId(t)).append("]: Relational\n  {\n")
                    .append("    ~mainTable ").append(db).append(table).append('\n');
            List<String> props = new ArrayList<>();
            entry.getValue().children.forEach((name, n) -> {
                if (!n.empty()) {
                    props.add(property(name, n, db, table, "    "));
                }
            });
            sb.append(String.join(",\n", props)).append(props.isEmpty() ? "" : "\n").append("  }\n");
        }
        for (TableSpec root : tables.stream().filter(t -> t.kind() == TableSpec.Kind.ROOT).toList()) {
            for (TableSpec child : schema.childrenOf(root)) {
                if (!tables.contains(child)) {
                    continue;
                }
                associationEnd(root, child).ifPresent(end -> {
                    String j = db + "@" + join(root, child);
                    sb.append("  ").append(end.association().qualifiedName()).append(": Relational\n  {\n    AssociationMapping\n    (\n")
                            .append("      ").append(end.navigable().name()).append('[').append(setId(root)).append(", ")
                            .append(setId(child)).append("]: ").append(j).append(",\n")
                            .append("      ").append(end.opposite().name()).append('[').append(setId(child)).append(", ")
                            .append(setId(root)).append("]: ").append(j).append("\n    )\n  }\n");
                });
            }
        }
        sb.append(")\n");
    }

    private static String setId(TableSpec t) {
        return t.name();
    }

    static String enumId(EnumDef e) {
        return e.qualifiedName().replace("::", "_");
    }

    private String property(String name, Node n, String db, String table, String indent) {
        if (n.column != null) {
            String enumMap = n.enumType == null ? "" : "EnumerationMapping " + enumId(n.enumType) + ": ";
            return indent + name + ": " + enumMap + db + table + "." + column(n.column.name());
        }
        List<String> inner = new ArrayList<>();
        n.children.forEach((k, c) -> {
            if (!c.empty()) {
                inner.add(property(k, c, db, table, indent + "  "));
            }
        });
        return indent + name + "\n" + indent + "(\n" + String.join(",\n", inner) + "\n" + indent + ")";
    }

    /** The property tree of a table's mappable columns (model columns that aren't JSON), with enum types resolved. */
    private Node tree(TableSpec t, Map<String, EnumDef> enums) {
        Node top = new Node();
        ClassDef cls = model.findClass(t.pureClass()).orElseThrow();
        boolean event = t.kind() == TableSpec.Kind.ROOT && t.smType().startsWith("evt_");
        for (ColumnSpec c : t.columns()) {
            if (c.source() != ColumnSpec.Source.MODEL || c.type() == ColumnType.JSON) {
                continue;
            }
            List<String> path = c.path();
            if (event) {
                if (path.isEmpty() || !path.getFirst().equals("payload")) {
                    continue; // lifecycle column: table access only
                }
                path = path.subList(1, path.size());
            }
            Optional<PropertyDef> leaf = resolve(cls, path);
            if (path.isEmpty() || leaf.isEmpty()) {
                continue;
            }
            Node n = top;
            for (String seg : path) {
                n = n.children.computeIfAbsent(seg, k -> new Node());
            }
            n.column = c;
            if (c.type() == ColumnType.ENUM) {
                EnumDef e = model.findEnum(leaf.get().type().name()).orElse(null);
                if (e == null) {
                    n.column = null; // an enum we can't resolve: leave the column to table access
                } else {
                    n.enumType = e;
                    enums.putIfAbsent(e.qualifiedName(), e);
                }
            }
        }
        return top;
    }

    /** The property at the end of {@code path} from {@code cls}, through properties and owned association ends. */
    private Optional<PropertyDef> resolve(ClassDef cls, List<String> path) {
        ClassDef at = cls;
        PropertyDef p = null;
        for (int i = 0; i < path.size(); i++) {
            String seg = path.get(i);
            ClassDef from = at;
            p = model.allProperties(from).stream().filter(x -> x.name().equals(seg)).findFirst()
                    .or(() -> model.associationProperties(from).stream().map(AssociationEnd::navigable)
                            .filter(x -> x.name().equals(seg)).findFirst())
                    .orElse(null);
            if (p == null) {
                return Optional.empty();
            }
            if (i + 1 < path.size()) {
                at = model.findClass(p.type().name()).orElse(null);
                if (at == null) {
                    return Optional.empty();
                }
            }
        }
        return Optional.ofNullable(p);
    }

    /** The association end of the root class whose property is the child table's array. */
    private Optional<AssociationEnd> associationEnd(TableSpec root, TableSpec child) {
        ClassDef cls = model.findClass(root.pureClass()).orElseThrow();
        String prop = child.sourcePath().getLast();
        return model.associationProperties(cls).stream().filter(e -> e.owned() && e.navigable().name().equals(prop)).findFirst();
    }
}
