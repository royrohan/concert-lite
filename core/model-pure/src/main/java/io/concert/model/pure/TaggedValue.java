package io.concert.model.pure;

import java.util.Objects;

/**
 * {@code {profile.tag = 'value'}}: a string annotation keyed by a tag of a {@link ProfileDef}.
 *
 * @param location where the value's string literal starts (its opening quote), or {@code null} when
 *     the value was not parsed from source. Not part of equality: two tagged values are equal when
 *     profile, tag and value are.
 */
public record TaggedValue(String profile, String tag, String value, SourceLocation location) {

    public TaggedValue(String profile, String tag, String value) {
        this(profile, tag, value, null);
    }

    /** The same tagged value with another profile name (location kept). */
    public TaggedValue withProfile(String qualifiedProfile) {
        return new TaggedValue(qualifiedProfile, tag, value, location);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TaggedValue t && Objects.equals(profile, t.profile) && Objects.equals(tag, t.tag)
                && Objects.equals(value, t.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, tag, value);
    }
}
