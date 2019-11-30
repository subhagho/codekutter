package com.codekutter.common.model.annotations;

import java.lang.annotation.*;

/**
 * Annotation used to define permission check hierarchy.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Heirarchy {
    /**
     * Permission check policy.
     *
     * Default is Override.
     * @return - Check Policy.
     */
    EPermissionPolicy policy() default EPermissionPolicy.Override;
}
