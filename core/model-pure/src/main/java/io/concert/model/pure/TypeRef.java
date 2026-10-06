package io.concert.model.pure;

import java.util.Objects;

/**
 * Reference to a property type. After parsing, {@code name} is exactly as written; after
 * {@link ModelResolver resolution} non-primitive names are fully qualified.
 */
public record TypeRef(String name, boolean primitive) {

    public TypeRef {
        Objects.requireNonNull(name, "name");
    }

    /** The primitive type, only valid when {@link #primitive()} is true. */
    public PrimitiveType primitiveType() {
        return PrimitiveType.fromPureName(name)
                .orElseThrow(() -> new IllegalStateException(name + " is not a primitive type"));
    }

    @Override
    public String toString() {
        return name;
    }
}
