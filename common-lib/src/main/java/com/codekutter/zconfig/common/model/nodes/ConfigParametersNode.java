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

import com.codekutter.zconfig.common.model.Configuration;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.List;

/**
 * Configuration node representing parameters to be specified for a path node.
 * Parameters are specified as key/value pairs enclosed in a {parameters} block.
 * <p>
 * Example:
 * JSON >>
 * <pre>
 *     parameters: {
 *         key1: value1,
 *         key2: value2,
 *         ...
 *     }
 * </pre>
 * XML >>
 * <pre>
 *     <parameters>
 *         <key1 value="value1" />
 *         <key2 value="value2" />
 *         ...
 *     </parameters>
 * </pre>
 */
public class ConfigParametersNode extends ConfigKeyValueNode {
    /**
     * Search abbreviation for parameter node.
     */
    public static final String NODE_ABBR_PREFIX = "#";

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigParametersNode() {
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigParametersNode(
            Configuration configuration,
            AbstractConfigNode parent) {
        super(configuration, parent);
    }

    /**
     * Override the setApplicationName method to set the name to the static node name.
     *
     * @param name - Configuration node name.
     */
    @Override
    public void setName(String name) {
        if (getConfiguration() != null)
            super.setName(getConfiguration().getSettings().getParametersNodeName());
        else {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
            super.setName(name);
        }
    }


    /**
     * Find the specified path under this configuration node.
     *
     * @param path - Dot separated path.
     * @return - Node at path
     */
    @Override
    public AbstractConfigNode find(String path) {
        return find(path, NODE_ABBR_PREFIX);
    }


    /**
     * Find a configuration node specified by the path/index.
     *
     * @param path  - Tokenized Path array.
     * @param index - Current index in the path array to search for.
     * @return - Configuration Node found.
     */
    @Override
    public AbstractConfigNode find(List<String> path, int index) {
        return find(path, index, NODE_ABBR_PREFIX);
    }

    /**
     * Get the node name to be used for DB records.
     *
     * @return - DB Node Name
     */
    @Override
    public String getDbNodeName() {
        return String.format("%s.%s", getConfiguration().getSettings().getParametersNodeName(), getName());
    }

    /**
     * Get the Search path to reach this node.
     *
     * @return - Node search path.
     */
    @Override
    public String getSearchPath() {
        Preconditions.checkNotNull(getParent());
        String path = getParent().getSearchPath();
        return String.format("%s%s", path, NODE_ABBR_PREFIX);
    }
}
