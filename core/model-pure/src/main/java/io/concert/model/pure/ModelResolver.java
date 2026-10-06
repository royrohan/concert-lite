package io.concert.model.pure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Merges one or more parsed models and checks them: type names resolve, no duplicate elements or
 * properties, sound inheritance, well-typed defaults and known profile members. All errors are
 * reported together in one {@link PureModelException}.
 *
 * <p>An unqualified type or profile name resolves first against the packages imported by the
 * source it appears in (more than one match there is an error), then falls back to the element of
 * that simple name if it is unique across all models.
 *
 * <p>References to a profile that is not defined in the model are accepted and kept as written, as
 * the profile may live in another model the code generator does not see; references to a defined
 * profile are rewritten to its qualified name.
 */
public final class ModelResolver {

    public ResolvedModel resolve(PureModel... models) {
        return new Resolution(models).run();
    }

    private static final class Resolution {

        private final List<PureModel> models;
        private final List<String> errors = new ArrayList<>();
        private final List<String> warnings = new ArrayList<>();
        private final Map<String, SourceLocation> elementNames = new HashMap<>();
        private final Map<String, ClassDef> classes = new LinkedHashMap<>();
        private final Map<String, EnumDef> enums = new LinkedHashMap<>();
        private final Map<String, ProfileDef> profiles = new LinkedHashMap<>();
        private final List<AssociationDef> associations = new ArrayList<>();
        private final List<FunctionDef> functions = new ArrayList<>();
        private final Map<String, List<String>> typesBySimpleName = new HashMap<>();
        private final Map<String, List<String>> profilesBySimpleName = new HashMap<>();
        /** Imports of the source each class, enum and association came from, by qualified name. */
        private final Map<String, List<String>> importsByElement = new HashMap<>();
        /** Imports in scope for the element currently being resolved. */
        private List<String> imports = List.of();
        /** Package of the element being resolved: its own package's names win over imports (as in Pure). */
        private String ownPackage = "";

        Resolution(PureModel... models) {
            this.models = List.of(models);
        }

        ResolvedModel run() {
            index();
            List<ClassDef> resolvedClasses = classes.values().stream().map(this::resolveClass).toList();
            resolvedClasses.forEach(c -> classes.put(c.qualifiedName(), c));
            List<AssociationDef> resolvedAssociations = associations.stream().map(this::resolveAssociation).toList();
            checkCycles();
            List<EnumDef> resolvedEnums = enums.values().stream().map(this::resolveEnum).toList();
            resolvedEnums.forEach(e -> enums.put(e.qualifiedName(), e));
            for (AssociationDef a : resolvedAssociations) {
                if (a.end1().multiplicity().isToMany() && a.end2().multiplicity().isToMany()) {
                    warnings.add(a.location() + ": association " + a.qualifiedName()
                            + " is many-to-many; neither end is owned for serialization");
                }
            }
            ResolvedModel model = new ResolvedModel(
                    List.copyOf(classes.values()),
                    List.copyOf(enums.values()),
                    resolvedAssociations,
                    List.copyOf(profiles.values()),
                    functions,
                    warnings);
            checkDuplicateProperties(model);
            if (!errors.isEmpty()) {
                throw new PureModelException(errors);
            }
            return model;
        }

        // ---- indexing ----

        private void index() {
            for (PureModel m : models) {
                m.classes().forEach(c -> {
                    if (register(c.qualifiedName(), c.location())) {
                        classes.put(c.qualifiedName(), c);
                        bySimpleName(typesBySimpleName, c.qualifiedName());
                        importsByElement.put(c.qualifiedName(), m.imports());
                    }
                });
                m.enums().forEach(e -> {
                    if (register(e.qualifiedName(), e.location())) {
                        enums.put(e.qualifiedName(), e);
                        bySimpleName(typesBySimpleName, e.qualifiedName());
                        importsByElement.put(e.qualifiedName(), m.imports());
                    }
                });
                m.associations().forEach(a -> {
                    if (register(a.qualifiedName(), a.location())) {
                        associations.add(a);
                        importsByElement.put(a.qualifiedName(), m.imports());
                    }
                });
                m.profiles().forEach(p -> {
                    if (register(p.name(), p.location())) {
                        profiles.put(p.name(), p);
                        bySimpleName(profilesBySimpleName, p.name());
                    }
                });
                // Functions may be overloaded, so they are not subject to the duplicate-name check.
                functions.addAll(m.functions());
            }
        }

        private boolean register(String name, SourceLocation loc) {
            SourceLocation first = elementNames.putIfAbsent(name, loc);
            if (first != null) {
                error(loc, "duplicate element '" + name + "' (first defined at " + first + ")");
                return false;
            }
            return true;
        }

        private static void bySimpleName(Map<String, List<String>> index, String qualifiedName) {
            index.computeIfAbsent(QualifiedNames.simpleName(qualifiedName), k -> new ArrayList<>()).add(qualifiedName);
        }

        private void enterScope(String elementName) {
            imports = importsByElement.getOrDefault(elementName, List.of());
            ownPackage = QualifiedNames.packageOf(elementName);
        }

        /** Candidates for an unqualified name among the imported packages. */
        private List<String> imported(List<String> candidates) {
            return candidates.stream().filter(c -> imports.contains(QualifiedNames.packageOf(c))).toList();
        }

        /**
         * Narrows the candidates for an unqualified name to the one in the element's own package, else
         * to the imported ones when any match, else keeps them all. Returns {@code null} after recording an error if several imports match.
         */
        private List<String> scoped(List<String> candidates, SourceLocation loc, String what) {
            if (candidates.size() > 1) {
                List<String> samePackage = candidates.stream().filter(c -> QualifiedNames.packageOf(c).equals(ownPackage)).toList();
                if (samePackage.size() == 1) {
                    return samePackage;
                }
            }
            List<String> fromImports = imported(candidates);
            if (fromImports.size() > 1) {
                error(loc, "ambiguous " + what + " among imports; candidates: " + String.join(", ", fromImports));
                return null;
            }
            return fromImports.isEmpty() ? candidates : fromImports;
        }

        /** Resolves a class or enum name; returns {@code null} (after recording an error) if it cannot. */
        private String lookupType(String name, SourceLocation loc, String context) {
            if (QualifiedNames.isQualified(name)) {
                if (classes.containsKey(name) || enums.containsKey(name)) {
                    return name;
                }
                error(loc, "unknown type '" + name + "' " + context);
                return null;
            }
            List<String> candidates = scoped(typesBySimpleName.getOrDefault(name, List.of()), loc,
                    "type '" + name + "' " + context);
            if (candidates == null) {
                return null;
            }
            if (candidates.size() == 1) {
                return candidates.getFirst();
            }
            if (candidates.isEmpty()) {
                error(loc, "unknown type '" + name + "' " + context);
            } else {
                error(loc, "ambiguous type '" + name + "' " + context + "; candidates: " + String.join(", ", candidates));
            }
            return null;
        }

        private TypeRef resolveType(TypeRef type, SourceLocation loc, String context) {
            if (type.primitive()) {
                return type;
            }
            String resolved = lookupType(type.name(), loc, context);
            return resolved == null ? type : new TypeRef(resolved, false);
        }

        // ---- classes and associations ----

        private ClassDef resolveClass(ClassDef c) {
            enterScope(c.qualifiedName());
            List<String> supers = new ArrayList<>();
            for (String s : c.superTypes()) {
                String resolved = lookupSuperType(s, c);
                if (resolved != null) {
                    supers.add(resolved);
                }
            }
            List<StereotypeRef> stereotypes = resolveStereotypes(c.stereotypes(), c.location());
            List<TaggedValue> tags = resolveTags(c.tags(), c.location());
            List<PropertyDef> properties = c.properties().stream()
                    .map(p -> resolveProperty(p, "for property '" + p.name() + "' of " + c.qualifiedName()))
                    .toList();
            List<DerivedPropertyDef> derived = c.derived().stream()
                    .map(d -> d.withReturnType(resolveType(d.returnType(), d.location(),
                            "for derived property '" + d.name() + "' of " + c.qualifiedName())))
                    .toList();
            return new ClassDef(
                    c.qualifiedName(), supers, properties, derived, c.constraints(), stereotypes, tags, c.location());
        }

        private EnumDef resolveEnum(EnumDef e) {
            enterScope(e.qualifiedName());
            List<EnumValueDef> values = e.values().stream()
                    .map(v -> new EnumValueDef(v.name(), resolveStereotypes(v.stereotypes(), v.location()),
                            resolveTags(v.tags(), v.location()), v.location()))
                    .toList();
            return new EnumDef(e.qualifiedName(), values, resolveStereotypes(e.stereotypes(), e.location()),
                    resolveTags(e.tags(), e.location()), e.location());
        }

        private String lookupSuperType(String name, ClassDef c) {
            String qualified = QualifiedNames.isQualified(name) ? name : null;
            if (qualified == null) {
                List<String> candidates = scoped(typesBySimpleName.getOrDefault(name, List.of()).stream()
                        .filter(classes::containsKey)
                        .toList(), c.location(), "supertype '" + name + "' for class " + c.qualifiedName());
                if (candidates == null) {
                    return null;
                }
                if (candidates.size() > 1) {
                    error(c.location(), "ambiguous supertype '" + name + "' for class " + c.qualifiedName()
                            + "; candidates: " + String.join(", ", candidates));
                    return null;
                }
                qualified = candidates.isEmpty() ? name : candidates.getFirst();
            }
            if (classes.containsKey(qualified)) {
                return qualified;
            }
            if (enums.containsKey(qualified)) {
                error(c.location(), "supertype '" + name + "' of class " + c.qualifiedName() + " is not a class");
            } else {
                error(c.location(), "unknown supertype '" + name + "' for class " + c.qualifiedName());
            }
            return null;
        }

        private PropertyDef resolveProperty(PropertyDef p, String context) {
            p = annotated(p);
            TypeRef type = resolveType(p.type(), p.location(), context);
            Literal def = p.defaultValue();
            if (def != null && (type.primitive() || isKnown(type.name()))) {
                def = checkDefault(p, type, def);
            }
            return p.withResolved(type, def);
        }

        private boolean isKnown(String name) {
            return classes.containsKey(name) || enums.containsKey(name);
        }

        private PropertyDef annotated(PropertyDef p) {
            return p.withAnnotations(resolveStereotypes(p.stereotypes(), p.location()), resolveTags(p.tags(), p.location()));
        }

        private AssociationDef resolveAssociation(AssociationDef a) {
            enterScope(a.qualifiedName());
            List<StereotypeRef> stereotypes = resolveStereotypes(a.stereotypes(), a.location());
            List<TaggedValue> tags = resolveTags(a.tags(), a.location());
            PropertyDef[] ends = {a.end1(), a.end2()};
            for (int i = 0; i < 2; i++) {
                PropertyDef end = annotated(ends[i]);
                TypeRef t = resolveType(end.type(), end.location(),
                        "for association end '" + end.name() + "' of " + a.qualifiedName());
                if (!t.primitive() && !classes.containsKey(t.name()) && !enums.containsKey(t.name())) {
                    ends[i] = end.withResolved(t, end.defaultValue());
                    continue; // unknown type already reported
                }
                if (!classes.containsKey(t.name())) {
                    error(end.location(), "association end '" + end.name() + "' of " + a.qualifiedName()
                            + " must refer to a class, found " + t.name());
                }
                if (end.defaultValue() != null) {
                    error(end.location(), "association end '" + end.name() + "' cannot have a default value");
                }
                ends[i] = end.withResolved(t, end.defaultValue());
            }
            return new AssociationDef(a.qualifiedName(), ends[0], ends[1], stereotypes, tags, a.location());
        }

        private void checkCycles() {
            Set<String> done = new HashSet<>();
            for (String name : classes.keySet()) {
                visit(name, new ArrayList<>(), done);
            }
        }

        private void visit(String name, List<String> path, Set<String> done) {
            int idx = path.indexOf(name);
            if (idx >= 0) {
                List<String> cycle = new ArrayList<>(path.subList(idx, path.size()));
                cycle.add(name);
                error(classes.get(name).location(), "inheritance cycle: " + String.join(" -> ", cycle));
                return;
            }
            if (!done.add(name)) {
                return;
            }
            path.add(name);
            for (String s : classes.get(name).superTypes()) {
                visit(s, path, done);
            }
            path.removeLast();
        }

        private record Member(String name, String owner, SourceLocation location) {}

        private void checkDuplicateProperties(ResolvedModel model) {
            Set<String> reported = new HashSet<>();
            for (ClassDef c : classes.values()) {
                Map<String, Member> seen = new HashMap<>();
                for (ClassDef ancestor : model.linearization(c)) {
                    List<Member> members = new ArrayList<>();
                    ancestor.properties().forEach(p -> members.add(new Member(p.name(), ancestor.qualifiedName(), p.location())));
                    ancestor.derived().forEach(d -> members.add(new Member(d.name(), ancestor.qualifiedName(), d.location())));
                    model.directAssociationProperties(ancestor.qualifiedName()).forEach(e -> members.add(
                            new Member(e.navigable().name(), e.association().qualifiedName(), e.navigable().location())));
                    for (Member m : members) {
                        Member first = seen.putIfAbsent(m.name(), m);
                        // Report each clashing pair once, not again for every subclass.
                        if (first != null && reported.add(first + "|" + m)) {
                            error(m.location(), "duplicate property '" + m.name() + "' in class " + c.qualifiedName()
                                    + "; also declared by " + first.owner() + " at " + first.location());
                        }
                    }
                }
            }
        }

        // ---- annotations and defaults ----

        /** Checks stereotypes against defined profiles and qualifies their profile names. */
        private List<StereotypeRef> resolveStereotypes(List<StereotypeRef> stereotypes, SourceLocation loc) {
            List<StereotypeRef> result = new ArrayList<>();
            for (StereotypeRef s : stereotypes) {
                ProfileDef p = findProfile(s.profile(), loc);
                if (p != null && !p.stereotypes().contains(s.value())) {
                    error(loc, "unknown stereotype '" + s.value() + "' in profile " + p.name());
                }
                result.add(p == null ? s : new StereotypeRef(p.name(), s.value()));
            }
            return result;
        }

        /** Checks tags against defined profiles and qualifies their profile names. */
        private List<TaggedValue> resolveTags(List<TaggedValue> tags, SourceLocation loc) {
            List<TaggedValue> result = new ArrayList<>();
            for (TaggedValue t : tags) {
                ProfileDef p = findProfile(t.profile(), loc);
                if (p != null && !p.tags().contains(t.tag())) {
                    error(t.location() != null ? t.location() : loc, "unknown tag '" + t.tag() + "' in profile " + p.name());
                }
                result.add(p == null ? t : t.withProfile(p.name()));
            }
            return result;
        }

        /** The defined profile a reference names, or {@code null} if it is external or ambiguous. */
        private ProfileDef findProfile(String name, SourceLocation loc) {
            if (QualifiedNames.isQualified(name)) {
                return profiles.get(name);
            }
            List<String> candidates = scoped(profilesBySimpleName.getOrDefault(name, List.of()), loc,
                    "profile '" + name + "'");
            return candidates != null && candidates.size() == 1 ? profiles.get(candidates.getFirst()) : null;
        }

        private Literal checkDefault(PropertyDef p, TypeRef type, Literal value) {
            if (value instanceof Literal.ListLit list) {
                if (!p.multiplicity().isToMany()) {
                    error(p.location(), "list default value for single-valued property '" + p.name() + "'");
                    return value;
                }
                List<Literal> checked = new ArrayList<>();
                for (Literal element : list.values()) {
                    checked.add(element instanceof Literal.ListLit
                            ? mismatch(p, type, element)
                            : checkScalar(p, type, element));
                }
                return new Literal.ListLit(checked);
            }
            return checkScalar(p, type, value);
        }

        private Literal checkScalar(PropertyDef p, TypeRef type, Literal value) {
            if (type.primitive()) {
                boolean ok = switch (type.primitiveType()) {
                    case STRING -> value instanceof Literal.StringLit;
                    case INTEGER -> value instanceof Literal.IntLit;
                    case FLOAT, DECIMAL -> value instanceof Literal.FloatLit;
                    case NUMBER -> value instanceof Literal.IntLit || value instanceof Literal.FloatLit;
                    case BOOLEAN -> value instanceof Literal.BoolLit;
                    case DATE, STRICT_DATE, DATE_TIME -> {
                        error(p.location(), "default values are not supported for " + type.name()
                                + " property '" + p.name() + "'");
                        yield true;
                    }
                };
                return ok ? value : mismatch(p, type, value);
            }
            EnumDef target = enums.get(type.name());
            if (target == null) {
                error(p.location(), "default values are not supported for class-typed property '" + p.name() + "'");
                return value;
            }
            if (!(value instanceof Literal.EnumLit lit)) {
                return mismatch(p, type, value);
            }
            String enumName = lit.enumName();
            if (!QualifiedNames.isQualified(enumName)) {
                List<String> all = typesBySimpleName.getOrDefault(enumName, List.of()).stream()
                        .filter(enums::containsKey)
                        .toList();
                List<String> fromImports = imported(all);
                List<String> candidates = fromImports.isEmpty() ? all : fromImports;
                enumName = candidates.contains(target.qualifiedName())
                        ? target.qualifiedName()
                        : candidates.stream().findFirst().orElse(enumName);
            }
            if (!enumName.equals(target.qualifiedName())) {
                error(p.location(), "default value for property '" + p.name() + "' must be a value of "
                        + target.qualifiedName() + ", found " + lit.enumName() + "." + lit.value());
                return value;
            }
            if (target.value(lit.value()).isEmpty()) {
                error(p.location(), "unknown enum value '" + lit.value() + "' for enum " + target.qualifiedName());
                return value;
            }
            return new Literal.EnumLit(enumName, lit.value());
        }

        private Literal mismatch(PropertyDef p, TypeRef type, Literal value) {
            error(p.location(), "default value for property '" + p.name() + "' must be " + type.name()
                    + ", found " + kind(value));
            return value;
        }

        private static String kind(Literal l) {
            return switch (l) {
                case Literal.StringLit _ -> "String";
                case Literal.IntLit _ -> "Integer";
                case Literal.FloatLit _ -> "Float";
                case Literal.BoolLit _ -> "Boolean";
                case Literal.EnumLit e -> "enum value " + e.enumName() + "." + e.value();
                case Literal.ListLit _ -> "list";
            };
        }

        private void error(SourceLocation loc, String message) {
            errors.add(loc + ": " + message);
        }
    }
}
