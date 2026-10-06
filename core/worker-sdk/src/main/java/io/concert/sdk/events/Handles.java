package io.concert.sdk.events;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** The event class an {@link EventHandler} applies (when it cannot be inferred, e.g. for lambdas). */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Handles {

    Class<?> value();

    /** The {@code eventType} name; default: the catalog's name for {@link #value()}, else its simple name. */
    String eventType() default "";
}
