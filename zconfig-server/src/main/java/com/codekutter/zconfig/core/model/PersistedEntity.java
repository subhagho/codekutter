/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright (c) $year
 * Date: 13/2/19 4:47 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core.model;

import com.codekutter.common.model.IEntity;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.springframework.lang.NonNull;

/**
 * Abstract base class for defining an entity that is persisted.
 *
 * @param <K> - Entity key type.
 * @param <T> - Entity type.
 */
@Getter
@Setter
public abstract class PersistedEntity<K, T> implements IEntity<K> {
    private K id;

    /**
     * Get the unique Key for this entity.
     *
     * @return - Entity Key.
     */
    @Override
    public K getKey() {
        return id;
    }

    /**
     * Override the default hashCode to generate an ID based hash code.
     *
     * @return - Generated Hash Code.
     */
    @Override
    public int hashCode() {
        return getHashCode();
    }

    /**
     * Override the equals to compute equality based on the ID attribute.
     *
     * @param o - Target instance to compare with.
     * @return - Is equal?
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
        if (o != null && (o instanceof PersistedEntity)) {
            PersistedEntity<K, T> source = (PersistedEntity<K, T>) o;
            if (compareKey(source) == 0) {
                return true;
            }
        }
        return super.equals(o);
    }

    /**
     * Compare this entity instance's key with the passed source.
     *
     * @param source - Source instance to compare with.
     * @return - (<0 key < source.key) (0 key == source.key) (>0 key > source.key)
     */
    public abstract int compareKey(PersistedEntity<K, T> source);

    /**
     * Get the computed hash code for this entity instance.
     *
     * @return - Hash Code.
     */
    @JsonIgnore
    public abstract int getHashCode();
}
