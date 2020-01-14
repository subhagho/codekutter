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
 * Date: 15/2/19 6:15 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core.model;

import com.codekutter.common.model.IKey;
import com.codekutter.common.model.ModifiedBy;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import org.springframework.lang.NonNull;

import javax.annotation.Nonnull;

/**
 * Base class for defining entities.
 *
 * @param <K> - Entity Key type.
 * @param <T> - Entity Type.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
              property = "@class")
public abstract class BaseEntity<K extends IKey, T> extends PersistedEntity<K, T> {
    /**
     * Owner of this application group.
     */
    private ModifiedBy owner;

    /**
     * Last updated user/timestamp.
     */
    private ModifiedBy updated;

    /**
     * Get the Application Group owner.
     *
     * @return - Group Owner
     */
    public ModifiedBy getOwner() {
        return owner;
    }

    /**
     * Set the Application Group owner.
     *
     * @param owner - Group Owner
     */
    public void setOwner(@NonNull ModifiedBy owner) {
        Preconditions.checkArgument(owner != null);

        this.owner = owner;
    }

    /**
     * Get the last update info.
     *
     * @return - Last updated by user/timestamp.
     */
    public ModifiedBy getUpdated() {
        return updated;
    }

    /**
     * Set the last update info.
     *
     * @param updated - Last updated by user/timestamp.
     */
    public void setUpdated(
            @Nonnull ModifiedBy updated) {
        this.updated = updated;
    }
}
