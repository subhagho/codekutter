package com.codekutter.r2db.driver;

import com.codekutter.common.model.IEntity;

import javax.persistence.CascadeType;
import javax.persistence.JoinColumns;
import javax.persistence.OneToMany;
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Inherited
public @interface Reference {
    /**
     * Target entity to JOIN with.
     *
     * @return - Target entity class.
     */
    Class<? extends IEntity> target();

    /**
     * Column set to join on.
     *
     * @return - Join Columns
     */
    JoinColumns columns();

    /**
     * (Optional) The operations that must be cascaded to
     * the target of the association.
     * <p> Defaults to no operations being cascaded.
     *
     * <p> When the target collection is a {@link java.util.Map
     * java.util.Map}, the <code>cascade</code> element applies to the
     * map value.
     */
    CascadeType[] cascade() default {};

    /**
     * (Optional) Whether to apply the remove operation to entities that have
     * been removed from the relationship and to cascade the remove operation to
     * those entities.
     * @since Java Persistence 2.0
     */
    boolean orphanRemoval() default false;
}
