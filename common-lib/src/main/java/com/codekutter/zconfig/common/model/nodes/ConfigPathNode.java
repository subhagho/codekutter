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

import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.ConfigurationSettings;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.codekutter.zconfig.common.model.ENodeState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class represents a configuration path node.
 * <p>
 * Configuration path(s) can be represented using a (.) or (/) notation while referencing.
 */
public class ConfigPathNode extends ConfigElementNode {

    /**
     * Map containing the child nodes.
     */
    private Map<String, AbstractConfigNode> children = new HashMap<>();

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigPathNode() {
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigPathNode(Configuration configuration,
                          AbstractConfigNode parent) {
        super(configuration, parent);
    }

    /**
     * Get the map of child nodes for this path element.
     *
     * @return - Map of child nodes.
     */
    public Map<String, AbstractConfigNode> getChildren() {
        return children;
    }


    /**
     * Set the map of child nodes for this path element.
     *
     * @param children - Map of child nodes.
     */
    public void setChildren(Map<String, AbstractConfigNode> children) {
        this.children = children;
    }

    /**
     * Add a new child node to this path element.
     * <p>
     * Note: Node names are assumed to be unique, adding a child node with an existing name will
     * override the element if already present.
     *
     * @param node - Child node to add.
     */
    public void addChildNode(AbstractConfigNode node) {
        Preconditions.checkArgument(node != null);

        if (children == null) {
            children = new HashMap<>();
        }
        node.setParent(this);
        children.put(node.getName(), node);
        updated();
    }

    /**
     * Get the child node (if any) with the specified node name.
     *
     * @param name - Node name to search for.
     * @return - Config Node, if found else NULL.
     */
    @JsonIgnore
    public AbstractConfigNode getChildNode(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        if (children != null) {
            return children.get(name);
        }
        return null;
    }

    /**
     * Remove the child node with the specified node name.
     *
     * @param name - Node name of the child to remvoe.
     * @return - True if removed, else False.
     */
    public boolean removeChildNode(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (children != null && children.containsKey(name)) {
            children.remove(name);
            updated();
            return true;
        }
        return false;
    }

    /**
     * Get the parameters, if any for this path node.
     *
     * @return - Parameters node, else NULL.
     */
    public ConfigParametersNode parmeters() {
        if (children != null &&
                children.containsKey(
                        getConfiguration().getSettings().getParametersNodeName())) {
            return (ConfigParametersNode) children
                    .get(getConfiguration().getSettings().getParametersNodeName());
        }
        return null;
    }

    /**
     * Get the properties, if any for this path node.
     *
     * @return - Properties node, else NULL.
     */
    public ConfigPropertiesNode properties() {
        if (children != null &&
                children.containsKey(
                        getConfiguration().getSettings().getPropertiesNodeName())) {
            return (ConfigPropertiesNode) children
                    .get(getConfiguration().getSettings().getPropertiesNodeName());
        }
        return null;
    }

    /**
     * Get the properties, if any for this path node.
     *
     * @return - Properties node, else NULL.
     */
    public ConfigAttributesNode attributes() {
        if (children != null &&
                children.containsKey(
                        getConfiguration().getSettings().getAttributesNodeName())) {
            return (ConfigAttributesNode) children
                    .get(getConfiguration().getSettings().getAttributesNodeName());
        }
        return null;
    }

    /**
     * Search for the path under this node.
     *
     * @param path  - Tokenized Path array.
     * @param index - Current index in the path array to search for.
     * @return - Node found or NULL.
     */
    @Override
    public AbstractConfigNode find(List<String> path, int index)
    throws ConfigurationException {
        String key = path.get(index);
        if (key.compareTo(ConfigurationSettings.NODE_PARENT_TERM) == 0) {
            if (getParent() != null) {
                return findInParent(path, index, index + 1);
            }
            return null;
        }
        if (getName().compareTo(key) == 0) {
            if (index == path.size() - 1) {
                return this;
            } else {
                return findChild(path, index);
            }
        } else if (ConfigurationSettings.isWildcard(key)) {
            if (index == path.size() - 1) {
                return this;
            } else {
                return findChild(path, index);
            }
        } else if (ConfigurationSettings.isRecursiveWildcard(key)) {
            String cname = path.get(index + 1);
            if (children.containsKey(cname)) {
                return children.get(cname).find(path, index + 1);
            } else {
                return findChild(path, index);
            }
        } else if (index == 0 && children.containsKey(key)) {
            if (path.size() == 1) {
                return children.get(key);
            } else
                return findChild(path, index);
        }

        return null;
    }

    /**
     * Find if any child node matches the required search criteria.
     *
     * @param path  - Path Array.
     * @param index - Index in the path array.
     * @return - Found node or NULL.
     */
    public AbstractConfigNode findChild(List<String> path, int index)
    throws ConfigurationException {
        String cname = path.get(index + 1);
        String name = path.get(index);
        if (children.containsKey(cname)) {
            AbstractConfigNode node = children.get(cname);
            return node.find(path, index + 1);
        } else if (ConfigurationSettings.isWildcard(cname)) {
            List<AbstractConfigNode> nodes = new ArrayList<>();
            if (index + 1 == path.size() - 1) {
                for (String ckey : children.keySet()) {
                    AbstractConfigNode cn = children.get(ckey);
                    nodes.add(cn);
                }
            } else {
                for (String ckey : children.keySet()) {
                    AbstractConfigNode cn = children.get(ckey);
                    AbstractConfigNode sn = cn.find(path, index + 1);
                    if (sn != null) {
                        nodes.add(sn);
                    }
                }
            }
            if (!nodes.isEmpty()) {
                if (nodes.size() > 1) {
                    ConfigSearchListNode nodeList =
                            new ConfigSearchListNode(getConfiguration(),
                                                     null);
                    for (AbstractConfigNode nn : nodes) {
                        nodeList.addValue(nn);
                    }
                    return nodeList;
                } else {
                    return nodes.get(0);
                }
            }
        } else if (ConfigurationSettings.isRecursiveWildcard(name)) {
            List<AbstractConfigNode> nodes = new ArrayList<>();
            for (String ckey : children.keySet()) {
                AbstractConfigNode cn = children.get(ckey);
                AbstractConfigNode sn = cn.find(path, index);
                if (sn != null) {
                    nodes.add(sn);
                }
            }
            if (!nodes.isEmpty()) {
                if (nodes.size() > 1) {
                    ConfigSearchListNode nodeList =
                            new ConfigSearchListNode(getConfiguration(),
                                                     null);
                    for (AbstractConfigNode nn : nodes) {
                        nodeList.addValue(nn);
                    }
                    return nodeList;
                } else {
                    return nodes.get(0);
                }
            }
        } else if (getParent() != null &&
                cname.compareTo(ConfigurationSettings.NODE_PARENT_TERM) == 0) {
            return findInParent(path, index, index + 2);
        }
        return null;
    }

    private AbstractConfigNode findInParent(List<String> path, int index, int offset)
    throws ConfigurationException {
        String pname = getParent().getName();
        StringBuffer buff = new StringBuffer(pname);
        for (int ii = offset; ii < path.size(); ii++) {
            buff.append(ConfigurationSettings.NODE_SEARCH_SEPERATOR)
                .append(path.get(ii));
        }
        return getParent().find(buff.toString());
    }

    /**
     * Override default toString(). Will print a path element with children if any.
     * Example: /path:[{child}, {child}...]
     *
     * @return - Path String.
     */
    @Override
    public String toString() {
        StringBuffer buff = new StringBuffer(String.format("/%s", getName()));
        if (children != null && !children.isEmpty()) {
            buff.append(":[");
            boolean first = true;
            for (String key : children.keySet()) {
                AbstractConfigNode node = children.get(key);
                if (node != null) {
                    if (first) {
                        first = false;
                    } else {
                        buff.append(", ");
                    }
                }
                if (node != null)
                    buff.append(String.format("{%s}", node.toString()));
            }
            buff.append("]");
        }
        return buff.toString();
    }

    /**
     * Update the state of this node and all the child nodes.
     *
     * @param state - New state.
     */
    @Override
    public void updateState(ENodeState state) {
        getState().setState(state);

        if (children != null && !children.isEmpty()) {
            for (String key : children.keySet()) {
                AbstractConfigNode node = children.get(key);
                if (node instanceof ConfigElementNode) {
                    ((ConfigElementNode) node).updateState(state);
                }
            }
        }
    }

    /**
     * Update the state of this node as Synced and for all the children.
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
        if (children != null && !children.isEmpty()) {
            for (String key : children.keySet()) {
                AbstractConfigNode node = children.get(key);
                node.loaded();
            }
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
        if (children != null && !children.isEmpty()) {
            for (String key : children.keySet()) {
                children.get(key).validate();
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
        if (children != null && !children.isEmpty()) {
            for (String key : children.keySet()) {
                children.get(key).changeConfiguration(configuration);
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
