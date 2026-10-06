package io.concert.model.pure;

import java.util.List;
import java.util.Objects;

/**
 * A stored property of a class, or an end of an association.
 *
 * @param defaultValue the declared default, or {@code null} when absent
 */
public record PropertyDef(
        String name,
        TypeRef type,
        Multiplicity multiplicity,
        Aggregation aggregation,
        Literal defaultValue,
        List<StereotypeRef> stereotypes,
        List<TaggedValue> tags,
        SourceLocation location) {

    public PropertyDef {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(multiplicity, "multiplicity");
        aggregation = aggregation == null ? Aggregation.NONE : aggregation;
        stereotypes = List.copyOf(stereotypes);
        tags = List.copyOf(tags);
    }

    PropertyDef withResolved(TypeRef resolvedType, Literal resolvedDefault) {
        return new PropertyDef(name, resolvedType, multiplicity, aggregation, resolvedDefault, stereotypes, tags, location);
    }

    PropertyDef withAnnotations(List<StereotypeRef> resolvedStereotypes, List<TaggedValue> resolvedTags) {
        return new PropertyDef(name, type, multiplicity, aggregation, defaultValue, resolvedStereotypes, resolvedTags, location);
    }
}
