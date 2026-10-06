package io.concert.model.pure;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A validated model with every type name fully qualified, produced by {@link ModelResolver}.
 *
 * <p><b>Association ownership</b> decides which side of a relationship carries the related objects
 * when an entity is serialized (the other side is a back-reference and is not serialized):
 *
 * <ul>
 *   <li>one-to-many: the to-many end is owned, e.g. {@code Order.lines}; {@code OrderLine.order} is
 *       the back-reference;
 *   <li>one-to-one: an end marked {@code (composite)} is owned; if neither or both are, {@code end2}
 *       is owned;
 *   <li>many-to-many: neither end is owned (related objects must be referenced by key) and a
 *       {@link #warnings() warning} is recorded.
 * </ul>
 */
public final class ResolvedModel {

    private final Map<String, ClassDef> classes;
    private final Map<String, EnumDef> enums;
    private final List<AssociationDef> associations;
    private final List<ProfileDef> profiles;
    private final List<FunctionDef> functions;
    private final List<String> warnings;
    private final Map<String, List<AssociationEnd>> directEnds = new LinkedHashMap<>();

    ResolvedModel(
            List<ClassDef> classes,
            List<EnumDef> enums,
            List<AssociationDef> associations,
            List<ProfileDef> profiles,
            List<FunctionDef> functions,
            List<String> warnings) {
        Map<String, ClassDef> c = new LinkedHashMap<>();
        classes.forEach(cd -> c.put(cd.qualifiedName(), cd));
        this.classes = c;
        Map<String, EnumDef> e = new LinkedHashMap<>();
        enums.forEach(ed -> e.put(ed.qualifiedName(), ed));
        this.enums = e;
        this.associations = List.copyOf(associations);
        this.profiles = List.copyOf(profiles);
        this.functions = List.copyOf(functions);
        this.warnings = List.copyOf(warnings);
        for (AssociationDef a : associations) {
            // end1 is a property of end2's class and vice versa.
            addEnd(a.end1().type().name(), new AssociationEnd(a, a.end2(), a.end1(), isOwned(a, true)));
            addEnd(a.end2().type().name(), new AssociationEnd(a, a.end1(), a.end2(), isOwned(a, false)));
        }
    }

    private void addEnd(String className, AssociationEnd end) {
        directEnds.computeIfAbsent(className, k -> new ArrayList<>()).add(end);
    }

    /** Applies the ownership rule documented on this class to {@code end2} or {@code end1}. */
    static boolean isOwned(AssociationDef a, boolean end2) {
        PropertyDef self = end2 ? a.end2() : a.end1();
        PropertyDef other = end2 ? a.end1() : a.end2();
        boolean selfMany = self.multiplicity().isToMany();
        boolean otherMany = other.multiplicity().isToMany();
        if (selfMany || otherMany) {
            return selfMany && !otherMany;
        }
        boolean selfComposite = self.aggregation() == Aggregation.COMPOSITE;
        boolean otherComposite = other.aggregation() == Aggregation.COMPOSITE;
        return selfComposite != otherComposite ? selfComposite : end2;
    }

    public List<ClassDef> classes() {
        return List.copyOf(classes.values());
    }

    public List<EnumDef> enums() {
        return List.copyOf(enums.values());
    }

    public List<AssociationDef> associations() {
        return associations;
    }

    public List<ProfileDef> profiles() {
        return profiles;
    }

    public List<FunctionDef> functions() {
        return functions;
    }

    /** Non-fatal findings, formatted {@code file:line:col: message}. */
    public List<String> warnings() {
        return warnings;
    }

    public Optional<ClassDef> findClass(String qualifiedName) {
        return Optional.ofNullable(classes.get(qualifiedName));
    }

    public Optional<EnumDef> findEnum(String qualifiedName) {
        return Optional.ofNullable(enums.get(qualifiedName));
    }

    public boolean isEnum(TypeRef type) {
        return !type.primitive() && enums.containsKey(type.name());
    }

    public boolean isClass(TypeRef type) {
        return !type.primitive() && classes.containsKey(type.name());
    }

    /**
     * The class followed by all its ancestors' linearization, supertypes first in declaration order,
     * each class once.
     */
    public List<ClassDef> linearization(ClassDef cls) {
        Set<String> seen = new LinkedHashSet<>();
        linearize(cls.qualifiedName(), seen);
        return seen.stream().map(classes::get).toList();
    }

    private void linearize(String name, Set<String> seen) {
        ClassDef c = classes.get(name);
        if (c == null || seen.contains(name)) {
            return;
        }
        // Guard against cycles before recursing; re-add afterwards so the class lands after its supertypes.
        seen.add(name);
        for (String s : c.superTypes()) {
            linearize(s, seen);
        }
        seen.remove(name);
        seen.add(name);
    }

    /** Own and inherited stored properties, supertypes first, in declaration order. */
    public List<PropertyDef> allProperties(ClassDef cls) {
        return linearization(cls).stream().flatMap(c -> c.properties().stream()).toList();
    }

    /** Own and inherited derived properties, supertypes first. */
    public List<DerivedPropertyDef> allDerivedProperties(ClassDef cls) {
        return linearization(cls).stream().flatMap(c -> c.derived().stream()).toList();
    }

    /**
     * Properties this class (or an ancestor) gets from associations, supertypes first. A
     * self-association contributes both ends.
     */
    public List<AssociationEnd> associationProperties(ClassDef cls) {
        return linearization(cls).stream()
                .flatMap(c -> directEnds.getOrDefault(c.qualifiedName(), List.of()).stream())
                .toList();
    }

    List<AssociationEnd> directAssociationProperties(String className) {
        return directEnds.getOrDefault(className, List.of());
    }
}
