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
 * Date: 16/2/19 11:41 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core.model.nodes;

import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import com.codekutter.zconfig.core.model.PersistedConfigPathNode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * Key/Value map node for configuration maps.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class PersistedConfigMapNode extends PersistedConfigPathNode {
    /**
     * Map of configuration key/value pairs.
     */
    private Map<String, String> map;

    /**
     * Get the configuration key/value map.
     *
     * @return - Key/Value map.
     */
    public Map<String, String> getMap() {
        return map;
    }

    /**
     * Set the configuration key/value map.
     *
     * @param map - Key/Value map.
     */
    public void setMap(Map<String, String> map) {
        this.map = map;
    }

    /**
     * Set the configuration key/value map.
     *
     * @param map - Key/Value map.
     */
    public void setMapFrom(Map<String, ConfigValueNode> map) {
        if (map != null && !map.isEmpty()) {
            this.map = new HashMap<>(map.size());
            for (String key : map.keySet()) {
                ConfigValueNode vn = map.get(key);
                if (vn != null) {
                    this.map.put(key, vn.getValue());
                }
            }
        }
    }

    /**
     * Get the configuration value for the specified key.
     *
     * @param key - Map key.
     * @return - Configuration value.
     */
    public String getValue(@Nonnull String key) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        if (map != null) {
            return map.get(key);
        }
        return null;
    }

    /**
     * Add the passed key/value to the configuration map.
     *
     * @param key   - Map key.
     * @param value - Configuration value.
     */
    public void addValue(@Nonnull String key, String value) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        if (map == null) {
            map = new HashMap<>();
        }
        map.put(key, value);
    }

    /**
     * Remove the configuration value for the specified key.
     *
     * @param key - Map key.
     * @return - Is removed?
     */
    public boolean removeValue(@Nonnull String key) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        if (map != null) {
            return !Strings.isNullOrEmpty(map.remove(key));
        }
        return false;
    }

    /**
     * Clear the configuration key/value map.
     */
    public void clear() {
        if (map != null) {
            map.clear();
        }
    }

    /**
     * Get the size of this key/value map.
     *
     * @return - Map size.
     */
    public int size() {
        if (map != null) {
            return map.size();
        }
        return 0;
    }

    /**
     * Check if the map is empty.
     *
     * @return - Is empty?
     */
    @JsonIgnore
    public boolean isEmpty() {
        if (map != null) {
            return map.isEmpty();
        }
        return true;
    }
}
