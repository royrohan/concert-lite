package io.concert.model.runtime;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the Pure model a handler works on. The annotation processor in {@code model-codegen}
 * generates Java classes for the model; the annotation is kept at runtime so the handler's model and
 * aggregate root can be discovered reflectively.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface LegendModel {

    /**
     * Path of the {@code .pure} file, relative to one of the configured model root directories. Set
     * exactly one of {@code file} and {@link #files()}.
     */
    String file() default "";

    /**
     * Several {@code .pure} files that are parsed and resolved together, e.g. a shared
     * {@code common.pure} plus the handler's own model. Set exactly one of {@link #file()} and
     * {@code files}. A file shared by several file sets is generated once; its generated classes must
     * not depend on the other files of a set (no subclasses or associations of its classes declared
     * elsewhere), otherwise the processor reports a conflict.
     */
    String[] files() default {};

    /** Qualified Pure name of the class that is the aggregate root for this handler. */
    String root();

    /** Java package for the generated classes; defaults to {@code <handler package>.model}. */
    String javaPackage() default "";
}
