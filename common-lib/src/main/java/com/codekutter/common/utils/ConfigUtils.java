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
 * Date: 4/3/19 11:10 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.utils;

import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.ConfigurationSettings;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Utility functions for configuration/configuration nodes.
 */
public class ConfigUtils {
    private static final String NODE_NAME_DESCRIPTION = "__description";
    private static final String ATTR_CLASS = "class";
    private static final String ATTR_NAME = "name";

    /**
     * Get the resolved search path for specified search string.
     *
     * @param path - Specified Search Path
     * @param settings - Configuration Settings.
     * @param node - Node to search under
     * @return - Resolved search path.
     * @throws ConfigurationException
     */
    public static List<String> getResolvedPath(@Nonnull String path,
                                               @Nonnull
                                                       ConfigurationSettings settings,
                                               AbstractConfigNode node)
    throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        Preconditions.checkArgument(settings != null);

        List<String> stack = new ArrayList<>();
        String[] parts = path.split(ConfigurationSettings.NODE_SEARCH_SEPERATOR);
        if (parts != null && parts.length > 0) {
            for (String part : parts) {
                if (Strings.isNullOrEmpty(part)) {
                    continue;
                }
                String[] pc = checkSubPath(part, settings);
                if (pc != null && pc.length > 0) {
                    for (String p : pc) {
                        stack.add(p);
                    }
                } else {
                    stack.add(part);
                }
            }
        } else {
            stack.add(path);
        }
        return stack;
    }

    /**
     * Check if the passed node name contains sub-tags for parameters/attributes.
     *
     * @param name - Path element to parse.
     * @return - Parsed path name, if tags are present, else NULL.
     */
    public static String[] checkSubPath(@Nonnull String name,
                                        @Nonnull ConfigurationSettings settings)
            throws
            ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkArgument(settings != null);

        Matcher matcher = ConfigurationSettings.INDEX_PATTERN.matcher(name);
        if (matcher != null && matcher.matches()) {
            String pname = matcher.group(1);
            String index = matcher.group(2);
            if (Strings.isNullOrEmpty(pname) || Strings.isNullOrEmpty(index)) {
                throw new ConfigurationException(String.format(
                        "Error getting name/index from array node. [term=%s]",
                        name));
            }
            return new String[]{pname, index};
        }
        int index = name.indexOf(ConfigurationSettings.PARAM_NODE_CHAR);
        if (index >= 0) {
            if (name.compareTo(ConfigurationSettings.PARAM_NODE_CHAR) == 0) {
                return new String[]{settings
                        .getParametersNodeName()};
            }
            String[] parts = name.split(ConfigurationSettings.PARAM_NODE_CHAR);
            if (index == 0) {
                return new String[]{settings
                        .getParametersNodeName(), parts[1]};
            }
            if (parts.length == 1) {
                return new String[]{parts[0],
                        settings
                                .getParametersNodeName()};
            } else if (parts.length == 2) {
                return new String[]{parts[0],
                        settings
                                .getParametersNodeName(),
                        parts[1]};
            }
        }
        index = name.indexOf(ConfigurationSettings.ATTR_NODE_CHAR);
        if (index >= 0) {
            if (name.compareTo(ConfigurationSettings.ATTR_NODE_CHAR) == 0) {
                return new String[]{settings
                        .getAttributesNodeName()};
            }
            String[] parts = name.split(ConfigurationSettings.ATTR_NODE_CHAR);
            if (index == 0) {
                return new String[]{settings
                        .getAttributesNodeName(), parts[1]};
            }
            if (parts.length == 1) {
                return new String[]{parts[0],
                        settings
                                .getAttributesNodeName()};
            } else if (parts.length == 2) {
                return new String[]{parts[0],
                        settings
                                .getAttributesNodeName(),
                        parts[1]};
            }
        }
        index = name.indexOf(ConfigurationSettings.PROP_NODE_CHAR);
        if (index >= 0) {
            if (name.compareTo(ConfigurationSettings.PROP_NODE_CHAR) == 0) {
                return new String[]{settings
                        .getPropertiesNodeName()};
            }
            String[] parts = name.split(
                    String.format("\\%s", ConfigurationSettings.PROP_NODE_CHAR));
            if (index == 0) {
                return new String[]{settings
                        .getPropertiesNodeName(), parts[1]};
            }
            if (parts.length == 1) {
                return new String[]{parts[0],
                        settings
                                .getPropertiesNodeName()};
            } else if (parts.length == 2) {
                return new String[]{parts[0],
                        settings
                                .getPropertiesNodeName(),
                        parts[1]};
            }
        }
        return null;
    }

    /**
     * Add a description element to the specified configuration node.
     *
     * @param node        - Configuration node
     * @param description - Description.
     */
    public static final void addDescription(@Nonnull AbstractConfigNode node,
                                            @Nonnull String description)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(description));
        if (node instanceof ConfigPathNode) {
            AbstractConfigNode cnode = node.find(
                    String.format("%s.%s", node.getName(), NODE_NAME_DESCRIPTION));
            if (cnode instanceof ConfigValueNode) {
                ((ConfigValueNode) cnode).setValue(description);
            } else {
                cnode = new ConfigValueNode(node.getConfiguration(), node);
                ((ConfigValueNode) cnode).setValue(description);
                ((ConfigPathNode) node).addChildNode(cnode);
            }
        } else if (node instanceof ConfigKeyValueNode) {
            ((ConfigKeyValueNode) node)
                    .addKeyValue(NODE_NAME_DESCRIPTION, description);
        }
    }

    /**
     * Extract the description of this node if present.
     *
     * @param node - Configuration node.
     * @return - Description.
     */
    public static final String getDescription(@Nonnull AbstractConfigNode node)
            throws ConfigurationException {
        if (node instanceof ConfigPathNode) {
            AbstractConfigNode cnode = node.find(
                    String.format("%s.%s", node.getName(), NODE_NAME_DESCRIPTION));
            if (cnode instanceof ConfigValueNode) {
                return ((ConfigValueNode) cnode).getValue();
            }
        } else if (node instanceof ConfigKeyValueNode) {
            if (((ConfigKeyValueNode) node).hasKey(NODE_NAME_DESCRIPTION)) {
                return ((ConfigKeyValueNode) node).getValue(NODE_NAME_DESCRIPTION)
                        .getValue();
            }
        }
        return null;
    }

    /**
     * Get the path annotation (if specified) for the type.
     *
     * @param type - Instance Type
     * @return - Path Annotation
     */
    public static final String getAnnotationPath(@Nonnull Class<?> type) {
        if (type.isAnnotationPresent(ConfigPath.class)) {
            ConfigPath path = type.getAnnotation(ConfigPath.class);
            if (path != null && !Strings.isNullOrEmpty(path.path())) {
                return path.path();
            }
        }
        return null;
    }

    /**
     * Search the node based on the path annotation specified in the type.
     *
     * @param type - Instance Type
     * @param node - Configuration node to search under.
     * @return - Node, if found.
     * @throws ConfigurationException
     */
    public static final AbstractConfigNode getPathNode(@Nonnull Class<?> type, @Nonnull ConfigPathNode node) throws ConfigurationException {
        String path = getAnnotationPath(type);
        if (!Strings.isNullOrEmpty(path)) {
            return node.find(path);
        }
        return null;
    }

    /**
     * Get the "class" attribute for the node.
     *
     * @param node - Configuration node.
     * @return - Class attribute value.
     */
    public static final String getClassAttribute(@Nonnull AbstractConfigNode node) {
        if (node instanceof ConfigPathNode) {
            ConfigAttributesNode attrs = ((ConfigPathNode) node).attributes();
            if (attrs.hasKey(ATTR_CLASS)) {
                ConfigValueNode vn = attrs.getValue(ATTR_CLASS);
                if (vn != null) {
                    return vn.getValue();
                }
            }
        }
        return null;
    }

    /**
     * Get the "name" attribute for the node.
     *
     * @param node - Configuration node.
     * @return - Name attribute value.
     */
    public static final String getNameAttribute(@Nonnull AbstractConfigNode node) {
        if (node instanceof ConfigPathNode) {
            ConfigAttributesNode attrs = ((ConfigPathNode) node).attributes();
            if (attrs.hasKey(ATTR_NAME)) {
                ConfigValueNode vn = attrs.getValue(ATTR_NAME);
                if (vn != null) {
                    return vn.getValue();
                }
            }
        }
        return null;
    }
}
