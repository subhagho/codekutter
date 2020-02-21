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
 * Date: 24/2/19 12:36 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.model.nodes;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.ENodeState;

import java.util.List;

/**
 * Class represent a configuration node of type list with String values.
 */
public class ConfigListValueNode extends ConfigListNode<ConfigValueNode> {

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigListValueNode() {
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigListValueNode(
            Configuration configuration,
            AbstractConfigNode parent) {
        super(configuration, parent);
    }

    /**
     * Find the value node in the list with the specified name.
     *
     * @param name - Name of node to find.
     * @return - Value node, else NULL.
     */
    public ConfigValueNode getValue(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        List<ConfigValueNode> values = getValues();
        if (values != null && !values.isEmpty()) {
            for (ConfigValueNode value : values) {
                if (value != null && value.getName().compareTo(name) == 0) {
                    return value;
                }
            }
        }
        return null;
    }


    /**
     * @param path  - Tokenized Path array.
     * @param index - Current index in the path array to search for.
     * @return - Node found or NULL.
     */
    @Override
    public AbstractConfigNode find(List<String> path, int index)
    throws ConfigurationException {
        String key = path.get(index);
        if (getName().compareTo(key) == 0) {
            if (index == path.size() - 1) {
                return this;
            } else if (!isEmpty()) {
                index = index + 1;
                List<ConfigValueNode> nodes = getValues();
                for (AbstractConfigNode node : nodes) {
                    AbstractConfigNode fn = node.find(path, index);
                    if (fn != null) {
                        return fn;
                    }
                }
            }
        }
        return null;
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
     * Mark the configuration instance has been completely loaded.
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
        List<ConfigValueNode> values = getValues();
        if (values != null && !values.isEmpty()) {
            for (ConfigValueNode v : values) {
                v.loaded();
            }
        }
    }

    /**
     * Get the node name to be used for DB records.
     *
     * @return - DB Node Name
     */
    @Override
    public String getDbNodeName() {
        return getName();
    }
}
