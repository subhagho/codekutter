/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.codekutter.common.stores.annotations;

import com.codekutter.common.model.IEntity;
import joptsimple.internal.Strings;

import javax.persistence.CascadeType;
import javax.persistence.JoinColumns;
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Inherited
@SuppressWarnings("rawtypes")
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

    /**
     * Query to be applied to the join condition.
     *
     * @return - Query String.
     */
    String query() default Strings.EMPTY;
}
