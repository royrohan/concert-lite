package io.concert.sink;

import io.concert.model.pure.AssociationEnd;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.ModelResolver;
import io.concert.model.pure.PropertyDef;
import io.concert.model.pure.PureModel;
import io.concert.model.pure.PureParser;
import io.concert.model.pure.ResolvedModel;
import io.concert.sink.ColumnSpec.Source;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Maps the Pure models of the state machines' aggregate roots to a {@link SchemaSpec}.
 *
 * <p>Per root (an smType and its root class, see {@link #parseRoots}):
 *
 * <ul>
 *   <li>a root table named after the smType, pluralized ({@code order -> orders}), with the system
 *       columns {@code entity_id} (primary key), {@code entity_version}, {@code sm_type},
 *       {@code sm_state}, {@code created_at}, {@code completed_at}, {@code model_json} and one typed
 *       column per stored to-one property: primitives by type, enums as {@link ColumnType#ENUM};
 *   <li>value classes (class-typed properties that are not associations, e.g. {@code total: Money})
 *       and owned to-one associations are <b>flattened</b> into prefixed columns ({@code total_amount},
 *       {@code total_currency}) up to {@value #MAX_DEPTH} levels deep: plain columns are easier to
 *       query than STRUCTs and every target supports them;
 *   <li>one child table per owned to-many association ({@code order_lines}) with {@code entity_id},
 *       {@code entity_version}, {@code idx} and the element's flattened columns. Only one level of
 *       child tables is generated: to-many values below it (and to-many primitives, enums or value
 *       classes anywhere) become {@link ColumnType#JSON} columns, and the full model is always in
 *       {@code model_json};
 *   <li>back-references (non-owned association ends) and derived properties are skipped;
 *   <li>an owned to-many association whose element class is the root class of <b>another configured
 *       smType</b> is not a child table and gets no column: its elements are aggregates of their own,
 *       stored as their own snapshots in their own root table and joined on their keys (e.g. trading's
 *       {@code Order.executions}, empty in every order snapshot, while {@code trading_executions} holds
 *       the executions). The array is still in {@code model_json}.
 * </ul>
 *
 * <p><b>Event-style roots.</b> An smType starting with {@value #EVENT_PREFIX} ({@code evt_order_create_event}) holds
 * event lifecycle rows, whose model JSON is {@code {eventId, eventType, status, attempts, ..., payload: {...}}} and whose
 * root class is the event (payload) class: the table gets the lifecycle columns ({@link #EVENT_COLUMNS}: event id, type,
 * domain, status, outcome, error, attempts, parent, causation root, depth, ...) and then the payload class's columns,
 * read below {@code payload}. Keyed-state documents ({@code st_<type>}) are plain roots: their model JSON is the state
 * class.
 *
 * Table names are deterministic: root {@code plural(snake(smType))} ({@code trading_order ->
 * trading_orders}), child {@code snake(smType) + "_" + snake(association)} ({@code order_lines}).
 *
 * A model property whose column name collides with a system column gets a {@code model_} prefix.
 */
public final class SchemaMapper {

    /** Default {@code SINK_ROOTS}: the sample workers' typed machines. */
    public static final String DEFAULT_ROOTS =
            "order:demo::order::Order,payment:demo::payment::Payment,shipment:demo::shipment::Shipment";

    static final int MAX_DEPTH = 3;

    private static final List<ColumnSpec> ROOT_SYSTEM = List.of(
            ColumnSpec.system("entity_id", ColumnType.STRING, Source.ENTITY_ID),
            ColumnSpec.system("entity_version", ColumnType.BIGINT, Source.ENTITY_VERSION),
            ColumnSpec.system("sm_type", ColumnType.STRING, Source.SM_TYPE),
            ColumnSpec.system("sm_state", ColumnType.STRING, Source.SM_STATE),
            ColumnSpec.system("created_at", ColumnType.TIMESTAMPTZ, Source.CREATED_AT),
            ColumnSpec.system("completed_at", ColumnType.TIMESTAMPTZ, Source.COMPLETED_AT),
            ColumnSpec.system("model_json", ColumnType.JSON, Source.MODEL_JSON));

    private static final List<ColumnSpec> CHILD_SYSTEM = List.of(
            ColumnSpec.system("entity_id", ColumnType.STRING, Source.ENTITY_ID),
            ColumnSpec.system("entity_version", ColumnType.BIGINT, Source.ENTITY_VERSION),
            ColumnSpec.system("idx", ColumnType.BIGINT, Source.IDX));

    /** smType prefix of event-style lifecycle rows ({@code EventRows.eventSmType}). */
    public static final String EVENT_PREFIX = "evt_";

    /** Field of a lifecycle row's data holding the event payload. */
    static final String EVENT_PAYLOAD = "payload";

    /**
     * The lifecycle columns of an event root, read from the row data ({@code EventRows.lifecycle}); the payload
     * class's columns follow (a payload property colliding with one of these gets the {@code model_} prefix).
     */
    static final List<ColumnSpec> EVENT_COLUMNS = List.of(
            eventColumn("event_id", ColumnType.STRING, "eventId"),
            eventColumn("event_type", ColumnType.STRING, "eventType"),
            eventColumn("domain", ColumnType.STRING, "domain"),
            eventColumn("status", ColumnType.ENUM, "status"),
            eventColumn("outcome", ColumnType.STRING, "outcome"),
            eventColumn("error", ColumnType.STRING, "error"),
            eventColumn("attempts", ColumnType.BIGINT, "attempts"),
            eventColumn("retries", ColumnType.BIGINT, "retries"),
            eventColumn("parent_event_id", ColumnType.STRING, "parentEventId"),
            eventColumn("causation_root", ColumnType.STRING, "causationRoot"),
            eventColumn("depth", ColumnType.BIGINT, "depth"),
            eventColumn("processor_key", ColumnType.STRING, "processorKey"),
            eventColumn("request_id", ColumnType.STRING, "requestId"),
            eventColumn("scheduled_at_ms", ColumnType.BIGINT, "scheduledAt"),
            eventColumn("lock_keys", ColumnType.JSON, "keys"),
            eventColumn("children", ColumnType.JSON, "children"));

    private static ColumnSpec eventColumn(String name, ColumnType type, String field) {
        return new ColumnSpec(name, type, Source.MODEL, List.of(field), null);
    }

    /** Whether snapshots of {@code smType} are event lifecycle rows (smType {@code evt_<event type>}). */
    public static boolean isEventRoot(String smType) {
        return smType.startsWith(EVENT_PREFIX);
    }

    private final ResolvedModel model;

    public SchemaMapper(ResolvedModel model) {
        this.model = model;
    }

    /** Parses and resolves every {@code *.pure} file in {@code dir} (non-recursive) as one model. */
    public static ResolvedModel loadModels(Path dir) {
        return loadModels(List.of(dir));
    }

    /**
     * {@code MODELS_DIR} form: one or more directories separated by commas (e.g.
     * {@code /models,/models-trading}), all resolved together as one model.
     */
    public static ResolvedModel loadModels(String dirs) {
        List<Path> paths = new ArrayList<>();
        for (String d : dirs.split(",")) {
            if (!d.isBlank()) {
                paths.add(Path.of(d.trim()));
            }
        }
        if (paths.isEmpty()) {
            throw new IllegalArgumentException("no model directory given");
        }
        return loadModels(paths);
    }

    /** Parses and resolves every {@code *.pure} file of every directory in {@code dirs} as one model. */
    public static ResolvedModel loadModels(List<Path> dirs) {
        List<PureModel> models = new ArrayList<>();
        for (Path dir : dirs) {
            try (Stream<Path> files = Files.list(dir)) {
                List<Path> pure = files.filter(p -> p.toString().endsWith(".pure")).sorted().toList();
                if (pure.isEmpty()) {
                    throw new IllegalArgumentException("no .pure files in " + dir.toAbsolutePath());
                }
                for (Path p : pure) {
                    models.add(PureParser.parse(p));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new ModelResolver().resolve(models.toArray(PureModel[]::new));
    }

    /**
     * A comma-separated setting assembled from several environment variables: {@code name} (or
     * {@code dflt} when it is unset or blank) followed by every variable named {@code name_<suffix>}, in
     * suffix order. Compose overrides use this to add to {@code SINK_ROOTS} and {@code MODELS_DIR}
     * without repeating (or clobbering) the base value, e.g. a generated ecosystem's
     * {@code SINK_ROOTS_INSURANCE} and {@code MODELS_DIR_INSURANCE}.
     */
    public static String envList(Map<String, String> env, String name, String dflt) {
        List<String> parts = new ArrayList<>();
        String base = env.get(name);
        parts.add(base == null || base.isBlank() ? dflt : base);
        env.keySet().stream().filter(k -> k.startsWith(name + "_")).sorted().forEach(k -> {
            String v = env.get(k);
            if (v != null && !v.isBlank()) {
                parts.add(v.trim());
            }
        });
        return String.join(",", parts.stream().filter(p -> p != null && !p.isBlank()).toList());
    }

    /** Parses {@code smType:qualified::Class,...} into smType to class, in order. */
    public static Map<String, String> parseRoots(String spec) {
        Map<String, String> roots = new LinkedHashMap<>();
        for (String entry : spec.split(",")) {
            String e = entry.trim();
            if (e.isEmpty()) {
                continue;
            }
            int colon = e.indexOf(':');
            if (colon <= 0 || colon == e.length() - 1 || e.charAt(colon + 1) == ':') {
                throw new IllegalArgumentException("bad root '" + e + "', expected smType:package::Class");
            }
            roots.put(e.substring(0, colon), e.substring(colon + 1));
        }
        return roots;
    }

    /** The schema for {@code roots} (smType to root class). */
    public SchemaSpec map(Map<String, String> roots) {
        List<TableSpec> tables = new ArrayList<>();
        Set<String> rootClasses = new HashSet<>(roots.values());
        for (var root : roots.entrySet()) {
            String smType = root.getKey();
            ClassDef cls = model.findClass(root.getValue()).orElseThrow(() -> new IllegalArgumentException(
                    "root class " + root.getValue() + " of " + smType + " is not in the models"));
            String rootTable = plural(snake(smType));
            List<ColumnSpec> columns = new ArrayList<>(ROOT_SYSTEM);
            List<TableSpec> children = new ArrayList<>();
            Columns rootCols = new Columns(columns);
            // Event-style lifecycle rows: the model JSON is {eventId, status, ..., payload: {...}}; the payload class's
            // columns are read below "payload".
            boolean event = isEventRoot(smType);
            List<String> base = event ? List.of(EVENT_PAYLOAD) : List.of();
            if (event) {
                EVENT_COLUMNS.forEach(c -> rootCols.add(c.name(), c.type(), c.path(), null));
            }
            addClass(cls, base, "", null, rootCols, 0, Set.of(cls.qualifiedName()), true);
            for (AssociationEnd end : model.associationProperties(cls)) {
                PropertyDef p = end.navigable();
                if (end.owned() && p.multiplicity().isToMany() && !isOtherRoot(p, cls, rootClasses)) {
                    ClassDef element = model.findClass(p.type().name()).orElseThrow();
                    List<ColumnSpec> childCols = new ArrayList<>(CHILD_SYSTEM);
                    addClass(element, List.of(), "", null, new Columns(childCols), 0, Set.of(element.qualifiedName()), false);
                    children.add(new TableSpec(snake(smType) + "_" + snake(p.name()), TableSpec.Kind.CHILD, smType,
                            element.qualifiedName(), rootTable, append(base, p.name()), childCols));
                }
            }
            tables.add(new TableSpec(rootTable, TableSpec.Kind.ROOT, smType, cls.qualifiedName(), null, List.of(), columns));
            tables.addAll(children);
        }
        return new SchemaSpec(tables);
    }

    /** Whether the association's element class is the root of another configured smType (see the class doc). */
    private boolean isOtherRoot(PropertyDef p, ClassDef owner, Set<String> rootClasses) {
        String element = model.findClass(p.type().name()).map(ClassDef::qualifiedName).orElse(p.type().name());
        return !element.equals(owner.qualifiedName()) && rootClasses.contains(element);
    }

    /** Column list with name de-duplication against system columns. */
    private record Columns(List<ColumnSpec> list) {

        void add(String name, ColumnType type, List<String> path, String group) {
            Set<String> taken = new HashSet<>();
            list.forEach(c -> taken.add(c.name()));
            String n = taken.contains(name) ? "model_" + name : name;
            list.add(new ColumnSpec(n, type, Source.MODEL, path, group));
        }
    }

    /**
     * Adds the columns of {@code cls}'s stored properties and owned associations. With
     * {@code childTables}, top-level owned to-many associations are skipped (they are child tables,
     * see {@link #map}); otherwise they, like nested ones, become JSON.
     */
    private void addClass(ClassDef cls, List<String> path, String prefix, String group, Columns out, int depth,
            Set<String> visiting, boolean childTables) {
        for (PropertyDef p : model.allProperties(cls)) {
            addProperty(p, path, prefix, group, out, depth, visiting);
        }
        for (AssociationEnd end : model.associationProperties(cls)) {
            PropertyDef p = end.navigable();
            if (!end.owned() || (childTables && depth == 0 && p.multiplicity().isToMany())) {
                continue; // back-reference, or a child table at the top level
            }
            addProperty(p, path, prefix, group, out, depth, visiting);
        }
    }

    private void addProperty(PropertyDef p, List<String> path, String prefix, String group, Columns out, int depth,
            Set<String> visiting) {
        String name = prefix + snake(p.name());
        List<String> propPath = append(path, p.name());
        if (p.multiplicity().isToMany()) {
            out.add(name, ColumnType.JSON, propPath, group);
        } else if (p.type().primitive()) {
            out.add(name, primitive(p), propPath, group);
        } else if (model.isEnum(p.type())) {
            out.add(name, ColumnType.ENUM, propPath, group);
        } else {
            ClassDef value = model.findClass(p.type().name()).orElseThrow();
            if (depth + 1 >= MAX_DEPTH || visiting.contains(value.qualifiedName())) {
                out.add(name, ColumnType.JSON, propPath, group);
            } else {
                Set<String> nested = new HashSet<>(visiting);
                nested.add(value.qualifiedName());
                String g = group == null ? p.name() : group + "." + p.name();
                addClass(value, propPath, name + "_", g, out, depth + 1, nested, false);
            }
        }
    }

    private static ColumnType primitive(PropertyDef p) {
        return switch (p.type().primitiveType()) {
            case STRING, DATE -> ColumnType.STRING;
            case INTEGER -> ColumnType.BIGINT;
            case FLOAT, NUMBER -> ColumnType.DOUBLE;
            case DECIMAL -> ColumnType.DECIMAL;
            case BOOLEAN -> ColumnType.BOOLEAN;
            case STRICT_DATE -> ColumnType.DATE;
            case DATE_TIME -> ColumnType.TIMESTAMPTZ;
        };
    }

    private static List<String> append(List<String> path, String name) {
        List<String> p = new ArrayList<>(path);
        p.add(name);
        return p;
    }

    /** {@code shippingAddress -> shipping_address}, {@code weightKg -> weight_kg}, {@code SKU2 -> sku2}. */
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

    /** Naive English plural, enough for table names: order -> orders, entry -> entries, box -> boxes. */
    static String plural(String word) {
        String w = word.toLowerCase(Locale.ROOT);
        if (w.endsWith("s") || w.endsWith("x") || w.endsWith("z") || w.endsWith("ch") || w.endsWith("sh")) {
            return word + "es";
        }
        if (w.endsWith("y") && w.length() > 1 && "aeiou".indexOf(w.charAt(w.length() - 2)) < 0) {
            return word.substring(0, word.length() - 1) + "ies";
        }
        return word + "s";
    }
}
