package io.concert.model.pure;

/** Position of a model element in its source file; used to point errors at the offending text. */
public record SourceLocation(String file, int line, int column) {

    @Override
    public String toString() {
        return file + ":" + line + ":" + column;
    }
}
