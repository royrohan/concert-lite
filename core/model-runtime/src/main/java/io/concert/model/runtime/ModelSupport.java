package io.concert.model.runtime;

import java.util.List;

/**
 * Checks and traversals shared by generated classes, so the generated {@code relink()} and
 * {@code collectValidationErrors} stay one line per property.
 */
public final class ModelSupport {

    private ModelSupport() {}

    /** Reports a missing value of a property with a lower bound of at least one. */
    public static void required(Object value, String path, String multiplicity, List<String> errors) {
        if (value == null) {
            errors.add(path + ": required " + multiplicity);
        }
    }

    /** Reports a list whose size is outside {@code [lower..upper]} (a null list counts as empty) and null elements. */
    public static void size(List<?> values, int lower, Integer upper, String path, String multiplicity, List<String> errors) {
        int n = values == null ? 0 : values.size();
        if (n < lower || (upper != null && n > upper)) {
            errors.add(path + ": expected " + multiplicity + " values but found " + n);
        }
        for (int i = 0; i < n; i++) {
            if (values.get(i) == null) {
                errors.add(path + "[" + i + "]: null element");
            }
        }
    }

    public static void validate(ModelObject child, String path, List<String> errors) {
        if (child != null) {
            child.collectValidationErrors(path, errors);
        }
    }

    public static void validateEach(List<? extends ModelObject> children, String path, List<String> errors) {
        if (children != null) {
            for (int i = 0; i < children.size(); i++) {
                validate(children.get(i), path + "[" + i + "]", errors);
            }
        }
    }

    public static void relink(ModelObject child) {
        if (child != null) {
            child.relink();
        }
    }

    public static void relinkEach(List<? extends ModelObject> children) {
        if (children != null) {
            for (ModelObject child : children) {
                relink(child);
            }
        }
    }
}
