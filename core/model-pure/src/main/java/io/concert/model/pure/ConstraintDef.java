package io.concert.model.pure;

/**
 * A class invariant. The expression is kept as raw source text; it is not evaluated here.
 *
 * @param name constraint name, or {@code null} for an unnamed constraint
 */
public record ConstraintDef(String name, String expression) {}
