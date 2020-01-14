package com.codekutter.common.auditing;

import joptsimple.internal.Strings;

import java.lang.annotation.*;

/**
 * Annotation to mark entities to be
 * audited for changes.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Audited {
    /**
     * Specify the logger name to audit to.
     *
     * @return - Logger name
     */
    String logger() default Strings.EMPTY;
}
