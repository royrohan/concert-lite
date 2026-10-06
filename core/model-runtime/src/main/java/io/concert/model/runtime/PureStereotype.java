package io.concert.model.runtime;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** A Pure stereotype carried over to a generated class or field, e.g. {@code shop::meta::Governance.reviewed}. */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD})
@Repeatable(PureStereotypes.class)
public @interface PureStereotype {

    /** {@code profile.stereotype}, with the profile qualified when it is defined in the model. */
    String value();
}
