package io.concert.model.pure;

import java.util.List;
import java.util.Optional;

/**
 * The elements parsed from one Pure source, unresolved: type names are as written.
 *
 * @param imports packages imported with {@code import a::b::*;}, as {@code a::b}, in source order.
 *     They scope unqualified names for every element of this source (all of its sections), not just
 *     the section they appear in.
 */
public record PureModel(
        List<ClassDef> classes,
        List<EnumDef> enums,
        List<AssociationDef> associations,
        List<ProfileDef> profiles,
        List<FunctionDef> functions,
        List<String> imports) {

    public PureModel {
        classes = List.copyOf(classes);
        enums = List.copyOf(enums);
        associations = List.copyOf(associations);
        profiles = List.copyOf(profiles);
        functions = List.copyOf(functions);
        imports = List.copyOf(imports);
    }

    public PureModel(
            List<ClassDef> classes,
            List<EnumDef> enums,
            List<AssociationDef> associations,
            List<ProfileDef> profiles,
            List<FunctionDef> functions) {
        this(classes, enums, associations, profiles, functions, List.of());
    }

    public Optional<ClassDef> findClass(String qualifiedName) {
        return classes.stream().filter(c -> c.qualifiedName().equals(qualifiedName)).findFirst();
    }

    public Optional<EnumDef> findEnum(String qualifiedName) {
        return enums.stream().filter(e -> e.qualifiedName().equals(qualifiedName)).findFirst();
    }

    public Optional<AssociationDef> findAssociation(String qualifiedName) {
        return associations.stream().filter(a -> a.qualifiedName().equals(qualifiedName)).findFirst();
    }

    public Optional<ProfileDef> findProfile(String name) {
        return profiles.stream().filter(p -> p.name().equals(name)).findFirst();
    }

    public List<FunctionDef> findFunctions(String qualifiedName) {
        return functions.stream().filter(f -> f.name().equals(qualifiedName)).toList();
    }
}
