package io.concert.ecosystem;

import java.util.List;

/** The models of an ecosystem have errors; each is formatted {@code file:line:col: message}. */
public final class EcosystemException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final List<String> errors;
    private final List<String> warnings;

    public EcosystemException(List<String> errors, List<String> warnings) {
        super(errors.size() + " error(s):\n" + String.join("\n", errors));
        this.errors = List.copyOf(errors);
        this.warnings = List.copyOf(warnings);
    }

    public List<String> errors() {
        return errors;
    }

    public List<String> warnings() {
        return warnings;
    }
}
