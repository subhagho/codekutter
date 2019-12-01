package com.codekutter.common.model;

import java.lang.annotation.*;

/**
 * Annotation to mark a field as required (not NULL or Empty)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Inherited
public @interface Validate {
    /**
     * Constraint definition to validate using.
     *
     * Default is Null/Empty check.
     *
     * @return - Get the validation constraint.
     */
    String constraint() default "com.codekutter.grant.model.NullOrEmpty";
}
