package io.concert.model.runtime;

import java.util.ArrayList;
import java.util.List;

/** Contract of every class generated from a Pure model. */
public interface ModelObject {

    /**
     * Appends multiplicity violations of this object and the objects it owns, each formatted
     * {@code <path>.<property>: <problem>}. List positions are zero-based, e.g.
     * {@code Order.lines[2].sku: required [1]}. Back-references and many-to-many ends are not checked:
     * they are not serialized and are rebuilt by {@link #relink()}.
     */
    void collectValidationErrors(String path, List<String> errors);

    /** Path-qualified multiplicity violations, rooted at this object's simple class name. */
    default List<String> validationErrors() {
        List<String> errors = new ArrayList<>();
        collectValidationErrors(getClass().getSimpleName(), errors);
        return errors;
    }

    /** Throws {@link ModelValidationException} listing every violation, if there are any. */
    default void validate() {
        List<String> errors = validationErrors();
        if (!errors.isEmpty()) {
            throw new ModelValidationException(errors);
        }
    }

    /**
     * Re-establishes back-references (e.g. {@code line.order}) for this object and, recursively, the
     * objects it owns. They are not serialized, so this must run after deserialization;
     * {@link ModelJson#read} does it.
     */
    void relink();
}
