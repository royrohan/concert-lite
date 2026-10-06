package io.concert.model.pure;

/**
 * A Pure function. Only the signature is structured; parameters and body are raw source text and
 * are never executed.
 */
public record FunctionDef(
        String name, String params, TypeRef returnType, Multiplicity multiplicity, String body, SourceLocation location) {}
