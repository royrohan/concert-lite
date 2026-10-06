package io.concert.model.pure;

import java.util.List;

/** A literal used as a property default value. */
public sealed interface Literal {

    record StringLit(String value) implements Literal {}

    record IntLit(long value) implements Literal {}

    record FloatLit(double value) implements Literal {}

    record BoolLit(boolean value) implements Literal {}

    /** {@code enumName.value}; the enum name is fully qualified after resolution. */
    record EnumLit(String enumName, String value) implements Literal {}

    record ListLit(List<Literal> values) implements Literal {
        public ListLit {
            values = List.copyOf(values);
        }
    }
}
