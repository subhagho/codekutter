package com.codekutter.zconfig.common.model.annotations;

import java.lang.annotation.*;

/**
 * Annotation to be used to define auto-invoked methods based on
 * configuration mapping for auto-wired
 * configuration elements.
 * Created by subho on 16/11/15.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.CONSTRUCTOR})
@Inherited
public @interface MethodInvoke {
    /**
     * Get the configuration node path defined for this type.
     *
     * @return - Configuration node path, if defined.
     */
    String path() default "";
}
