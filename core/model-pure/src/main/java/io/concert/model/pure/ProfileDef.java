package io.concert.model.pure;

import java.util.List;

/** Declares the stereotypes and tags that elements may be annotated with. */
public record ProfileDef(String name, List<String> stereotypes, List<String> tags, SourceLocation location) {

    public ProfileDef {
        stereotypes = List.copyOf(stereotypes);
        tags = List.copyOf(tags);
    }
}
