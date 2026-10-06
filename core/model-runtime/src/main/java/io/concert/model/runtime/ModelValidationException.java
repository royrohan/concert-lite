package io.concert.model.runtime;

import java.util.List;

/** A model object violates the multiplicities declared in its Pure model. */
public final class ModelValidationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final List<String> errors;

    public ModelValidationException(List<String> errors) {
        super(String.join("\n", errors));
        this.errors = List.copyOf(errors);
    }

    public List<String> errors() {
        return errors;
    }
}
