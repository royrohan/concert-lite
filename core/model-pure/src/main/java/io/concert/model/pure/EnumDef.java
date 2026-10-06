package io.concert.model.pure;

import java.util.List;
import java.util.Optional;

/** A Pure enumeration. */
public record EnumDef(
        String qualifiedName,
        List<EnumValueDef> values,
        List<StereotypeRef> stereotypes,
        List<TaggedValue> tags,
        SourceLocation location) {

    public EnumDef {
        values = List.copyOf(values);
        stereotypes = List.copyOf(stereotypes);
        tags = List.copyOf(tags);
    }

    public String packageName() {
        return QualifiedNames.packageOf(qualifiedName);
    }

    public String simpleName() {
        return QualifiedNames.simpleName(qualifiedName);
    }

    public Optional<EnumValueDef> value(String name) {
        return values.stream().filter(v -> v.name().equals(name)).findFirst();
    }
}
