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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Class represents a array of node elements.
 * <p>
 * Node elements are expected to be of type
 * AbstractConfigNode or Strings (for value node).
 *
 * @param <T> - Element type
 */
public abstract class ConfigListNode<T extends AbstractConfigNode>
        extends ConfigElementNode {
    private List<T> values;

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigListNode() {
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigListNode(Configuration configuration,
                          AbstractConfigNode parent) {
        super(configuration, parent);
    }

    /**
     * Get the list of element values.
     *
     * @return - List of element values.
     */
    public List<T> getValues() {
        return values;
    }

    /**
     * Set the list of element values.
     *
     * @param values - List of element values.
     */
    public void setValues(List<T> values) {
        Preconditions.checkArgument(values != null);
        Preconditions.checkArgument(!values.isEmpty());

        this.values = values;
        updated();
    }

    /**
     * Add a new element value to the list.
     *
     * @param value - Element value to add.
     */
    public void addValue(T value) {
        Preconditions.checkArgument(value != null);
        if (values == null) {
            values = new ArrayList<>();
        }

        values.add(value);
        updated();
    }

    /**
     * Remove the specified value from the list.
     *
     * @param value - Element value to remove.
     */
    public void removeValue(T value) {
        Preconditions.checkArgument(value != null);
        if (values != null) {
            if (values.remove(value)) {
                updated();
            }
        }
    }

    /**
     * Get the element value at the specified index.
     *
     * @param index - Index to get value at.
     * @return - Element value at index, else throws java.lang.ArrayIndexOutOfBoundsException
     */
    public T getValue(int index) {
        Preconditions.checkArgument(index >= 0);
        if (values != null && index < values.size()) {
            return values.get(index);
        }
        throw new ArrayIndexOutOfBoundsException(
                "Values is NULL or specified index is out of bounds.");
    }

    /**
     * Get the size of the List in this node.
     *
     * @return - List Size.
     */
    public int size() {
        if (values != null) {
            return values.size();
        }
        return 0;
    }

    /**
     * Check if this list is empty (values == null or empty list).
     *
     * @return - Is empty?
     */
    @JsonIgnore
    public boolean isEmpty() {
        return (values == null || values.isEmpty());
    }


    /**
     * Validate that this node has been setup correctly.
     *
     * @throws ConfigurationException
     */
    @Override
    public void validate() throws ConfigurationException {
        super.validate();
        if (isEmpty()) {
            throw new ConfigurationException("No List elements loaded.");
        }
        if (!isEmpty()) {
            List<T> values = getValues();
            for (T value : values) {
                value.validate();
            }
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
        if (!isEmpty()) {
            List<T> values = getValues();
            for (T value : values) {
                value.changeConfiguration(configuration);
            }
        }
    }
}
