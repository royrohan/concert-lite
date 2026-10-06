package io.concert.model.pure;

import java.util.Optional;

/** The Pure primitive types a property may use directly, without a class or enum definition. */
public enum PrimitiveType {
    STRING("String"),
    INTEGER("Integer"),
    FLOAT("Float"),
    DECIMAL("Decimal"),
    BOOLEAN("Boolean"),
    DATE("Date"),
    STRICT_DATE("StrictDate"),
    DATE_TIME("DateTime"),
    NUMBER("Number");

    private final String pureName;

    PrimitiveType(String pureName) {
        this.pureName = pureName;
    }

    public String pureName() {
        return pureName;
    }

    public static Optional<PrimitiveType> fromPureName(String name) {
        for (PrimitiveType t : values()) {
            if (t.pureName.equals(name)) {
                return Optional.of(t);
            }
        }
        return Optional.empty();
    }
}
