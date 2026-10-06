package io.concert.model.pure;

import java.util.List;

/**
 * A bidirectional relationship between two classes. Each end is the property that the class at the
 * <em>other</em> end gets: in {@code Association A { order: Order[1]; lines: OrderLine[*]; }},
 * {@code Order} gets {@code lines} and {@code OrderLine} gets {@code order}.
 */
public record AssociationDef(
        String qualifiedName,
        PropertyDef end1,
        PropertyDef end2,
        List<StereotypeRef> stereotypes,
        List<TaggedValue> tags,
        SourceLocation location) {

    public AssociationDef {
        stereotypes = List.copyOf(stereotypes);
        tags = List.copyOf(tags);
    }
}
