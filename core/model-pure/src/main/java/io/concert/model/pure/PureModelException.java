package io.concert.model.pure;

import java.util.List;

/**
 * Semantic errors found while resolving a model. All errors are collected so a user can fix them
 * in one pass; each is formatted {@code file:line:col: message}.
 */
public final class PureModelException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final List<String> errors;

    public PureModelException(List<String> errors) {
        super(String.join("\n", errors));
        this.errors = List.copyOf(errors);
    }

    public List<String> errors() {
        return errors;
    }
}
