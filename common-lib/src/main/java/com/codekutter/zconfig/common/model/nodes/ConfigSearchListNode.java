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
 * Date: 24/2/19 12:52 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.model.nodes;

import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.ENodeState;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class ConfigSearchListNode extends ConfigListNode<AbstractConfigNode> {
    public static final String NODE_NAME = "SearchResult";

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigSearchListNode() {
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigSearchListNode(
            Configuration configuration,
            AbstractConfigNode parent) {
        super(configuration, parent);
        setName(NODE_NAME);
    }

    /**
     * Set the name of this configuration node.
     *
     * @param name - Configuration node name.
     */
    @Override
    public void setName(String name) {
        super.setName(NODE_NAME);
    }

    /**
     * Find a configuration node specified by the path/index.
     *
     * @param path  - Tokenized Path array.
     * @param index - Current index in the path array to search for.
     * @return - Configuration Node found.
     */
    @Override
    public AbstractConfigNode find(List<String> path, int index)
    throws ConfigurationException {
        List<AbstractConfigNode> values = getValues();
        if (values != null && !values.isEmpty()) {
            List<AbstractConfigNode> result = new ArrayList<>();
            for (AbstractConfigNode node : values) {
                AbstractConfigNode rs = node.find(path, index);
                if (rs != null) {
                    result.add(rs);
                }
            }
            if (!result.isEmpty()) {
                if (result.size() > 1) {
                    ConfigSearchListNode nodeList =
                            new ConfigSearchListNode(getConfiguration(),
                                                     null);
                    for (AbstractConfigNode nn : result) {
                        nodeList.addValue(nn);
                    }
                    return nodeList;
                } else {
                    return result.get(0);
                }
            }
        }
        return null;
    }

    /**
     * Find the specified path under this configuration node.
     *
     * @param path - Unix Path separated path.
     * @return - Node at path
     */
    @Override
    public AbstractConfigNode find(@Nonnull String path)
    throws ConfigurationException {
        ConfigSearchListNode result = new ConfigSearchListNode();
        List<AbstractConfigNode> nodes = getValues();
        if (nodes != null && !nodes.isEmpty()) {
            for (AbstractConfigNode node : nodes) {
                AbstractConfigNode r = node.find(path);
                if (r != null) {
                    result.addValue(r);
                }
            }
        }
        if (!result.isEmpty()) {
            if (result.size() == 1) {
                return result.getValue(0);
            }
            return result;
        }
        return null;
    }

    /**
     * Update the node states recursively to the new state.
     *
     * @param state - New state.
     */
    @Override
    public void updateState(ENodeState state) {
        throw new RuntimeException("Method should not be called.");
    }

    /**
     * Mark the configuration instance has been completely loaded.
     *
     * @throws ConfigurationException
     */
    @Override
    public void loaded() throws ConfigurationException {
        throw new RuntimeException("Method should not be called.");
    }

    /**
     * Get the node name to be used for DB records.
     *
     * @return - DB Node Name
     */
    @Override
    public String getDbNodeName() {
        return null;
    }
}
