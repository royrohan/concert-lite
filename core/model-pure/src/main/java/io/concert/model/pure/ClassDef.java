package io.concert.model.pure;

import java.util.List;
import java.util.Objects;

/** A Pure class: the shape of an entity or event payload. */
public record ClassDef(
        String qualifiedName,
        List<String> superTypes,
        List<PropertyDef> properties,
        List<DerivedPropertyDef> derived,
        List<ConstraintDef> constraints,
        List<StereotypeRef> stereotypes,
        List<TaggedValue> tags,
        SourceLocation location) {

    public ClassDef {
        Objects.requireNonNull(qualifiedName, "qualifiedName");
        superTypes = List.copyOf(superTypes);
        properties = List.copyOf(properties);
        derived = List.copyOf(derived);
        constraints = List.copyOf(constraints);
        stereotypes = List.copyOf(stereotypes);
        tags = List.copyOf(tags);
    }

    /** {@code a::b} for {@code a::b::Order}; empty for an unpackaged name. */
    public String packageName() {
        return QualifiedNames.packageOf(qualifiedName);
    }

    public String simpleName() {
        return QualifiedNames.simpleName(qualifiedName);
    }
}
