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
 * Date: 16/2/19 11:32 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core.model.nodes;

import com.codekutter.zconfig.core.model.PersistedConfigPathNode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * ZooKeeper node that stores a configuration value list.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
              property = "@class")
public class PersistedConfigListValueNode extends PersistedConfigPathNode {
    /**
     * List of configuration values.
     */
    private List<String> values;

    /**
     * Get the list of values.
     *
     * @return - List of values.
     */
    public List<String> getValues() {
        return values;
    }

    /**
     * Set the list of values.
     *
     * @param values - List of values.
     */
    public void setValues(List<String> values) {
        this.values = values;
    }

    /**
     * Get the configuration value at index.
     *
     * @param index - List index.
     * @return - Configuration value.
     */
    public String getValue(int index) {
        if (values != null) {
            return values.get(index);
        }
        return null;
    }

    /**
     * Add the passed configuration value to the list.
     *
     * @param value - Configuration value.
     */
    public void addValue(@Nonnull String value) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        if (values == null) {
            values = new ArrayList<>();
        }
        values.add(value);
    }

    /**
     * Remove the specified value from the list.
     *
     * @param value - Configuration value to remove.
     * @return - Is removed?
     */
    public boolean removeValue(@Nonnull String value) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        if (values != null) {
            return values.remove(value);
        }
        return false;
    }

    /**
     * Clear the value list.
     */
    public void clear() {
        if (values != null) {
            values.clear();
            values = null;
        }
    }

    /**
     * Get the size of this value list.
     *
     * @return - List size.
     */
    public int size() {
        if (values != null) {
            return values.size();
        }
        return 0;
    }

    /**
     * Check if this value list is empty.
     *
     * @return - Is empty?
     */
    @JsonIgnore
    public boolean isEmpty() {
        if (values != null) {
            return values.isEmpty();
        }
        return true;
    }
}
