package io.concert.model.pure;

/**
 * A qualified (computed) property, {@code name(params) { body } : Type[mult]}. Parameters and body
 * are kept as raw source text; they are not evaluated here.
 */
public record DerivedPropertyDef(
        String name, String params, String body, TypeRef returnType, Multiplicity multiplicity, SourceLocation location) {

    DerivedPropertyDef withReturnType(TypeRef resolved) {
        return new DerivedPropertyDef(name, params, body, resolved, multiplicity, location);
    }
}
