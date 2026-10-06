package io.concert.model.runtime;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** A Pure tagged value carried over to a generated class or field. */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD})
@Repeatable(PureTags.class)
public @interface PureTag {

    /** The profile, qualified when it is defined in the model. */
    String profile();

    String tag();

    String value();
}
