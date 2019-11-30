package com.codekutter.common.model.annotations;

import java.lang.annotation.*;

/**
 * Annotation to be used to specify copy type
 * for Entity Attributes.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Inherited
public @interface CopyType {
    /**
     * Get the Copy Type.
     * default = Ignore.
     *
     * @return - Copy Type.
     */
    ECopyType type() default ECopyType.Ignore;
}
