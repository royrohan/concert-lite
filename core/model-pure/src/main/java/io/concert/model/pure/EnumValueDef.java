package io.concert.model.pure;

import java.util.List;

/** One value of an {@link EnumDef}. */
public record EnumValueDef(String name, List<StereotypeRef> stereotypes, List<TaggedValue> tags, SourceLocation location) {

    public EnumValueDef {
        stereotypes = List.copyOf(stereotypes);
        tags = List.copyOf(tags);
    }
}
