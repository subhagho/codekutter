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

package com.codekutter.zconfig.common.model.nodes;

import com.codekutter.common.model.EReaderType;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.ENodeSource;
import com.codekutter.zconfig.common.model.ENodeState;
import com.codekutter.zconfig.common.model.Version;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.File;
import java.net.URI;
import java.util.List;

/**
 * Configuration node that represents an external configuration set to be included
 * under the current path.
 * <p>
 * Included configurations are expected to be complete configuration sets.
 */
public class ConfigIncludeNode extends ConfigElementNode {
    /**
     * Include node name.
     */
    public static final String NODE_NAME = "include";
    /**
     * Node specifying the Reader Type.
     */
    public static final String NODE_TYPE = "type";
    /**
     * Node specifying the resource path.
     */
    public static final String NODE_PATH = "path";
    /**
     * Node specifying the configuration version.
     */
    public static final String NODE_VERSION = "version";
    /**
     * Node specifying the configuration name.
     */
    public static final String NODE_CONFIG_NAME = "configName";

    /**
     * Name of this configuration.
     */
    private String configName;
    /**
     * Path (HTTP/File) of the configuration.
     */
    private String path;

    /**
     * The included child node.
     */
    private ConfigPathNode node;
    /**
     * Reader type to use for loading the configuration
     */
    private EReaderType readerType;

    /**
     * The configuration version to load.
     */
    private Version version;

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigIncludeNode() {
        setName(NODE_NAME);
        setNodeSource(ENodeSource.File);
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigIncludeNode(
            Configuration configuration,
            AbstractConfigNode parent) {
        super(configuration, parent);
        setName(NODE_NAME);
        setNodeSource(ENodeSource.File);
    }

    /**
     * Get the name of this configuration.
     *
     * @return - Configuration name.
     */
    public String getConfigName() {
        return configName;
    }

    /**
     * Get the name of this configuration.
     *
     * @param configName - Configuration name.
     */
    public void setConfigName(String configName) {
        this.configName = configName;
    }

    /**
     * Get the path of the configuration source.
     *
     * @return - Source Path.
     */
    public String getPath() {
        return path;
    }

    /**
     * Set the path of the configuration source.
     *
     * @param path - Source Path.
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * Get the included node.
     *
     * @return - Included node.
     */
    public ConfigPathNode getNode() {
        return node;
    }

    /**
     * Set the included node.
     *
     * @param node - Included node.
     */
    public void setNode(ConfigPathNode node) {
        Preconditions.checkArgument(node != null);
        this.node = node;
        this.node.setParent(this);
    }

    /**
     * Get the reader type specified for this included configuration.
     *
     * @return - Reader type.
     */
    public EReaderType getReaderType() {
        return readerType;
    }

    /**
     * Set the reader type specified for this included configuration.
     *
     * @param readerType - Reader type.
     */
    public void setReaderType(EReaderType readerType) {
        this.readerType = readerType;
    }

    /**
     * Get the configuration version.
     *
     * @return - Version
     */
    public Version getVersion() {
        return version;
    }

    /**
     * Set the configuration version.
     *
     * @param version - Version
     */
    public void setVersion(Version version) {
        this.version = version;
    }

    /**
     * Delegate the call to the included node.
     *
     * @param path  - Tokenized Path array.
     * @param index - Current index in the path array to search for.
     * @return - Node found or NULL.
     */
    @Override
    public AbstractConfigNode find(List<String> path, int index)
            throws ConfigurationException {
        return node.find(path, index);
    }

    /**
     * Update the state for this node and the embedded node.
     *
     * @param state - New state.
     */
    @Override
    public void updateState(ENodeState state) {
        getState().setState(state);
        if (node != null) {
            node.updateState(state);
        }
    }

    /**
     * Get the URI for this reader type/path.
     *
     * @return - Parsed URI.
     * @throws ConfigurationException
     */
    public URI getURI() throws ConfigurationException {
        if (readerType != null && !Strings.isNullOrEmpty(path)) {
            String pp = path;
            if (readerType == EReaderType.File) {
                File file = new File(pp);
                return file.toURI();
            } else {
                String uriString = String.format("%s://%s",
                        EReaderType
                                .getURIScheme(readerType),
                        pp);
                return URI.create(uriString);
            }
        }
        return null;
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
        if (node != null) {
            node.loaded();
        }
    }

    /**
     * Validate that this node has been setup correctly.
     *
     * @throws ConfigurationException
     */
    @Override
    public void validate() throws ConfigurationException {
        super.validate();
        if (Strings.isNullOrEmpty(configName)) {
            throw ConfigurationException.propertyNotFoundException("configName");
        }
        if (readerType == null) {
            throw ConfigurationException.propertyNotFoundException("readerType");
        }
        if (version == null) {
            throw ConfigurationException.propertyNotFoundException("version");
        }
        if (node == null) {
            throw new ConfigurationException("No included configuration loaded.");
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

    /**
     * Get the Search path to reach this node.
     *
     * @return - Node search path.
     */
    @Override
    public String getSearchPath() {
        Preconditions.checkNotNull(getParent());
        Preconditions.checkNotNull(node);
        String path = getParent().getSearchPath();
        return String.format("%s.%s", path, node.getName());
    }
}
