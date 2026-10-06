package io.concert.model.pure;

/**
 * Cardinality of a property, {@code [lower..upper]}. A {@code null} upper bound means unbounded
 * ({@code *}).
 */
public record Multiplicity(int lower, Integer upper) {

    public static final Multiplicity ONE = new Multiplicity(1, 1);
    public static final Multiplicity ZERO_ONE = new Multiplicity(0, 1);
    public static final Multiplicity MANY = new Multiplicity(0, null);
    public static final Multiplicity ONE_MANY = new Multiplicity(1, null);

    public Multiplicity {
        if (lower < 0) {
            throw new IllegalArgumentException("lower bound must be >= 0: " + lower);
        }
        if (upper != null && upper < lower) {
            throw new IllegalArgumentException("upper bound " + upper + " is less than lower bound " + lower);
        }
    }

    public boolean isToOne() {
        return upper != null && upper == 1;
    }

    public boolean isToMany() {
        return upper == null || upper > 1;
    }

    public boolean isOptional() {
        return lower == 0;
    }

    public boolean isRequired() {
        return lower > 0;
    }

    @Override
    public String toString() {
        if (upper == null) {
            return lower == 0 ? "[*]" : "[" + lower + "..*]";
        }
        return lower == upper ? "[" + lower + "]" : "[" + lower + ".." + upper + "]";
    }
}
