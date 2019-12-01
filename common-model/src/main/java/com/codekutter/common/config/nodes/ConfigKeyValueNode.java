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
 * Date: 24/2/19 12:33 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.config.nodes;

import com.codekutter.common.config.Configuration;
import com.codekutter.common.config.ConfigurationException;
import com.codekutter.common.config.ENodeState;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract Configuration node represents Key/Value pairs.
 */
public abstract class ConfigKeyValueNode extends ConfigElementNode {

    /**
     * Map containing the defined parameters within a node definition.
     */
    private Map<String, ConfigValueNode> keyValues;

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigKeyValueNode() {
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigKeyValueNode(
            Configuration configuration,
            AbstractConfigNode parent) {
        super(configuration, parent);
    }

    /**
     * Get the defined parameters for a specific node.
     *
     * @return - Map of parameters.
     */
    public Map<String, ConfigValueNode> getKeyValues() {
        return keyValues;
    }

    /**
     * Set the parameters for a specific node.
     *
     * @param keyValues - Map of parameters.
     */
    public void setKeyValues(Map<String, ConfigValueNode> keyValues) {
        updated();
        this.keyValues = keyValues;
    }

    /**
     * Get the value for the specified key.
     *
     * @param key - Parameter key.
     * @return - Parameter value or NULL if not found.
     */
    @JsonIgnore
    public ConfigValueNode getValue(String key) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        if (keyValues != null) {
            return keyValues.get(key);
        }
        return null;
    }

    /**
     * Check if the specified key exists in the Map.
     *
     * @param key - Key to look for.
     * @return - Exists?
     */
    public boolean hasKey(String key) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        if (keyValues != null) {
            return keyValues.containsKey(key);
        }
        return false;
    }

    /**
     * Check if this node is empty (doesn't have any key/values).
     *
     * @return - Is empty?
     */
    public boolean isEmpty() {
        return (keyValues == null || keyValues.isEmpty());
    }

    /**
     * Add all the key/values to this map.
     *
     * @param map - Key/Value map.
     */
    public void addAll(Map<String, ConfigValueNode> map) {
        Preconditions.checkArgument(map != null);
        if (!map.isEmpty()) {
            if (keyValues == null) {
                keyValues = new HashMap<>(map);
            } else {
                keyValues.putAll(map);
            }
        }
    }

    /**
     * Add a new key/value with the specified key and value.
     *
     * @param key   - Parameter key.
     * @param value - Parameter value.
     */
    public void addKeyValue(String key, String value) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        if (keyValues == null) {
            keyValues = new HashMap<>();
        }
        ConfigValueNode vn = new ConfigValueNode(getConfiguration(), getParent());
        vn.setConfiguration(getConfiguration());
        vn.setName(key);
        vn.setParent(this);
        vn.setValue(value);

        keyValues.put(key, vn);
        updated();
    }

    /**
     * Add the passed Value Node to this Map.
     *
     * @param node - Value Node.
     */
    public void addKeyValue(ConfigValueNode node) {
        Preconditions.checkArgument(node != null);
        if (keyValues == null) {
            keyValues = new HashMap<>();
        }
        keyValues.put(node.getName(), node);
        updated();
    }

    /**
     * Remove the key/value with the specified key.
     *
     * @param key - Parameter key.
     * @return - True if removed, else NULL.
     */
    public boolean removeKeyValue(String key) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        if (keyValues != null && !keyValues.isEmpty()) {
            if (keyValues.containsKey(key)) {
                keyValues.remove(key);
                updated();
                return true;
            }
        }
        return false;
    }


    /**
     * Find the specified path under this configuration node.
     *
     * @param path - Dot separated path.
     * @param abbr - Short hand notation for search
     * @return - Node at path
     */
    protected AbstractConfigNode find(String path, String abbr) {
        if (path.startsWith(abbr)) {
            path = path.substring(1);
        }
        if (hasKey(path)) {
            return getValue(path);
        }
        return null;
    }


    /**
     * Find a configuration node specified by the path/index.
     *
     * @param path  - Tokenized Path array.
     * @param index - Current index in the path array to search for.
     * @param abbr  - Short hand notation for search
     * @return - Configuration Node found.
     */
    public AbstractConfigNode find(List<String> path, int index, String abbr) {
        String key = path.get(index);
        if (!Strings.isNullOrEmpty(key)) {
            if (getName().compareTo(key) == 0) {
                if (index == path.size() - 1) {
                    return this;
                } else {
                    String pname = path.get(index + 1);
                    if (hasKey(pname)) {
                        return getValue(pname);
                    }
                }
            } else if (index == path.size() - 1) {
                if (key.startsWith(abbr)) {
                    key = key.substring(1);
                }
                if (hasKey(key)) {
                    return getValue(key);
                }
            }
        }
        return null;
    }

    /**
     * Prints the key/value pairs.
     *
     * @return - Key/Value pairs string.
     */
    @Override
    public String toString() {
        return String.format("%s:[key/values=%s]", getName(), keyValues);
    }

    /**
     * Update the state of this node.
     *
     * @param state - New state.
     */
    @Override
    public void updateState(ENodeState state) {
        getState().setState(state);
    }

    /**
     * Update the state of this node as Synced.
     *
     * @throws ConfigurationException
     */
    @Override
    public void loaded() throws ConfigurationException {
        if (getState().hasError()) {
            throw new ConfigurationException(String.format(
                    "Cannot mark as loaded : Object state is in error. [state=%s]",
                    getState().getState().name()));
        }
        updateState(ENodeState.Synced);
    }

    /**
     * Validate that this node has been setup correctly.
     *
     * @throws ConfigurationException
     */
    @Override
    public void validate() throws ConfigurationException {
        super.validate();
        if (keyValues == null || keyValues.isEmpty()) {
            throw new ConfigurationException("Missing Key/Values : NULL or Empty.");
        }
        for (String key : keyValues.keySet()) {
            ConfigValueNode vn = keyValues.get(key);
            vn.validate();
        }
    }

    /**
     * Change the configuration instance this node belongs to.
     * Used for included configurations.
     *
     * @param configuration - Changed configuration.
     */
    @Override
    public void changeConfiguration(Configuration configuration) {
        setConfiguration(configuration);
        for (String key : keyValues.keySet()) {
            ConfigValueNode vn = keyValues.get(key);
            vn.changeConfiguration(configuration);
        }
    }
}
