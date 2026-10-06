package io.concert.ecosystem;

import io.concert.model.pure.SourceLocation;
import java.util.ArrayList;
import java.util.List;

/** Collects errors and warnings, each formatted {@code file:line:col: message}. */
final class Diagnostics {

    final List<String> errors = new ArrayList<>();
    final List<String> warnings = new ArrayList<>();

    void error(SourceLocation at, String message) {
        errors.add(format(at, message));
    }

    void warn(SourceLocation at, String message) {
        warnings.add(format(at, "warning: " + message));
    }

    private static String format(SourceLocation at, String message) {
        return (at == null ? "<unknown>" : at.toString()) + ": " + message;
    }
}
