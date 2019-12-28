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
 * Date: 2/1/19 10:08 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.parsers;

import com.codekutter.common.model.EReaderType;
import com.codekutter.common.model.ModifiedBy;
import com.codekutter.common.utils.CypherUtils;
import com.codekutter.common.utils.IOUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.RemoteFileHelper;
import com.codekutter.zconfig.common.*;
import com.codekutter.zconfig.common.model.*;
import com.codekutter.zconfig.common.model.nodes.*;
import com.codekutter.zconfig.common.readers.AbstractConfigReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Configuration Parser implementation that reads the configuration from a JSON file.
 */
public class JSONConfigParser extends AbstractConfigParser {

    private Map<Integer, JsonNode> processedNodes = null;

    /**
     * Parse the configuration from the JSON file specified in the properties.
     * <p>
     * Sample JSON configuration:
     * <pre>
     *     {
     *         "header" : {
     *             "id" : "[configuration ID],
     *             "group" : "[application group]",
     *             "application" : [application]",
     *             "name" : "[configuration name]",
     *             "version" : "[version string]",
     *             "createdBy" : {
     *                 "user " : "[username]",
     *                 "timestamp" : "[datetime]"
     *             },
     *             "updatedBy" : {
     *                  "user " : "[username]",
     *                 "timestamp" : "[datetime]"
     *             }
     *         },
     *         "configuration" : {
     *              "properties" : {
     *                  "prop1" : "[value1]",
     *                  "prop2" : "[value2]",
     *                  ...
     *              },
     *              "sample" : {
     *                  "name" : "[node name]",
     *                  "version" : "[version string]",
     *                  "createdBy" : {
     *                      "user " : "[username]",
     *                      "timestamp" : "[datetime]"
     *                  },
     *                  "updatedBy" : {
     *                      "user " : "[username]",
     *                      "timestamp" : "[datetime]"
     *                  },
     *                  "parameters" : {
     *                      "param1" : "[value1]",
     *                      "param2" : "[value2]",
     *                      ...
     *                  },
     *                  "sample2": {
     *                      "attributes" : {
     *                          "attr1" : "[value1]",
     *                          ...
     *                          ...
     *                      }
     *                  }
     *             }
     *         }
     *     }
     * </pre>
     *
     * @param name     - Configuration name being loaded.
     * @param reader   - Configuration reader handle to read input from.
     * @param settings - Configuration Settings to use for parsing.
     * @param version  - Configuration version to load.
     * @throws ConfigurationException
     */
    @Override
    public void parse(String name, AbstractConfigReader reader,
                      ConfigurationSettings settings,
                      Version version, String password)
    throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkArgument(reader != null);
        Preconditions.checkArgument(version != null);

        if (processedNodes == null) {
            processedNodes = new HashMap<>();
        }

        try {
            if (!reader.isOpen()) {
                reader.open();
            }

            try (BufferedReader br = reader.getBufferedStream()) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(br);

                if (settings != null) {
                    this.settings = settings;
                } else {
                    this.settings = new ConfigurationSettings();
                }
                parse(name, version, rootNode, password);

                if (!Strings.isNullOrEmpty(configuration.getEncryptionHash())) {
                    ConfigKeyVault.getInstance().save(password, configuration);
                }

                // Call the load finish handler.
                doPostLoad();
            }
        } catch (JsonProcessingException e) {
            if (configuration != null)
                configuration.getState().setError(e);
            throw new ConfigurationException(e);
        } catch (IOException e) {
            if (configuration != null)
                configuration.getState().setError(e);
            throw new ConfigurationException((e));
        } catch (ConfigurationException e) {
            if (configuration != null)
                configuration.getState().setError(e);
            throw e;
        } catch (Exception e) {
            if (configuration != null)
                configuration.getState().setError(e);
            throw new ConfigurationException(e);
        }
    }

    /**
     * Check if node has already been processed.
     *
     * @param node - JSON node to check for.
     * @return - Is processed?
     */
    private boolean isProcessed(JsonNode node) {
        int hashCode = System.identityHashCode(node);
        if (processedNodes.containsKey(hashCode)) {
            return true;
        }
        processedNodes.put(hashCode, node);
        return false;
    }

    /**
     * Parse the configuration from the specified JSON node.
     *
     * @param name    - Expected Configuration name.
     * @param version - Expected Compatible version.
     * @param node    - JSON Root node.
     * @throws ConfigurationException
     */
    private synchronized void parse(String name,
                                    Version version, JsonNode node, String password)
    throws ConfigurationException {
        configuration = new Configuration(settings);
        configuration.getState().setState(ENodeState.Loading);
        configuration.setName(name);

        // Parse the configuration header.
        parseHeader(node, version, password);

        // Parse the configuration body
        parseBody(node, password);

        configuration.getRootConfigNode().updateState(ENodeState.Synced);
    }

    /**
     * Parse the JSON body to load the configuration.
     *
     * @param node - JSON node to load from.
     * @throws ConfigurationException
     */
    private void parseBody(JsonNode node, String password)
    throws ConfigurationException {
        Iterator<Map.Entry<String, JsonNode>> nodes = node.fields();
        if (nodes != null) {
            while (nodes.hasNext()) {
                Map.Entry<String, JsonNode> nn = nodes.next();
                if (nn.getKey().compareTo(JSONConfigConstants.CONFIG_HEADER_NODE) ==
                        0) {
                    continue;
                }
                parseConfiguration(nn.getKey(), nn.getValue(), password);
                break;
            }
        }
    }

    /**
     * Read the root configuration node and then the child nodes.
     *
     * @param name - Name of the root node.
     * @param node - Root configuration node.
     * @throws ConfigurationException
     */
    private void parseConfiguration(String name, JsonNode node, String password)
    throws ConfigurationException {
        if (node.getNodeType() != JsonNodeType.OBJECT) {
            throw new ConfigurationException(String.format(
                    "Invalid Configuration Node : [expected=%s][actual=%s]",
                    JsonNodeType.OBJECT.name(), node.getNodeType().name()));
        }
        // Read the root configuration node.
        ConfigPathNode rootConfigNode = new ConfigPathNode(configuration, null);
        rootConfigNode.setName(name);
        rootConfigNode.loading();

        configuration.setRootConfigNode(rootConfigNode);

        // Read the child nodes.
        parseChildNodes(rootConfigNode, node, password);
    }

    /**
     * Parse this JSON node and create a configuration element.
     *
     * @param name   - Element name
     * @param node   - JSON node to read from.
     * @param parent - Parent configuration node.
     * @throws ConfigurationException
     */
    private void parseNode(String name, JsonNode node, AbstractConfigNode parent,
                           String password)
    throws ConfigurationException {
        if (node.getNodeType() == JsonNodeType.OBJECT) {
            AbstractConfigNode nn = readObjectNode(name, node, parent, password);
            if (nn != null) {
                isProcessed(node);
            } else {
                throw new ConfigurationException("Error reading object node.");
            }
        } else if (node.getNodeType() == JsonNodeType.STRING) {
            if (parent instanceof ConfigKeyValueNode) {
                ((ConfigKeyValueNode) parent).addKeyValue(name, node.textValue());
            } else {
                ConfigValueNode cv = new ConfigValueNode(configuration, parent);
                cv.setName(name);
                cv.setParent(parent);
                cv.setConfiguration(configuration);
                cv.setValue(node.textValue());

                if (parent instanceof ConfigPathNode) {
                    ((ConfigPathNode) parent).addChildNode(cv);
                } else if (parent instanceof ConfigListValueNode) {
                    ((ConfigListValueNode) parent).addValue(cv);
                } else {
                    throw new ConfigurationException(String.format(
                            "Cannot add string value to parent node. [type=%s]",
                            parent.getClass().getCanonicalName()));
                }
            }
            isProcessed(node);
        } else if (node.getNodeType() == JsonNodeType.ARRAY) {
            readArrayNode(name, parent, (ArrayNode) node, password);
        }
    }

    /**
     * Read and process the specified JSON Array node.
     *
     * @param name      - Name of the node.
     * @param parent    - Parent Config Node.
     * @param arrayNode - JSON Array node.
     * @throws ConfigurationException
     */
    private void readArrayNode(String name, AbstractConfigNode parent,
                               ArrayNode arrayNode, String password)
    throws ConfigurationException {
        // Check if array element types are consistent
        JsonNodeType type = null;

        if (arrayNode.size() > 0) {
            for (int ii = 0; ii < arrayNode.size(); ii++) {
                JsonNode nn = arrayNode.get(ii);
                if (type == null) {
                    type = nn.getNodeType();
                } else {
                    if (nn.getNodeType() != type) {
                        throw new ConfigurationException(String.format(
                                "Invalid Array Element : [expected type=%s][actual type=%s]",
                                type.name(),
                                nn.getNodeType().name()));
                    }
                }
            }
        }
        if (type != null) {
            if (type == JsonNodeType.STRING) {
                ConfigListValueNode nn =
                        new ConfigListValueNode(configuration, parent);
                parseArrayNodes(name, nn, parent, arrayNode, password);
            } else if (type == JsonNodeType.OBJECT) {
                ConfigListElementNode nn =
                        new ConfigListElementNode(configuration, parent);
                parseArrayNodes(name, nn, parent, arrayNode, password);
            } else {
                throw new ConfigurationException(String.format(
                        "Unsupported Array element type. [type=%s]",
                        type.name()));
            }
        }
        isProcessed(arrayNode);
    }

    /**
     * Read the list elements for this Array Node.
     *
     * @param name      - List Node name.
     * @param listNode  - Parent List Config Node.
     * @param parent    - Parent Path node of the List.
     * @param arrayNode - JSON Array node to read from.
     * @param <T>       - Type
     * @throws ConfigurationException
     */
    private <T extends AbstractConfigNode> void parseArrayNodes(String name,
                                                                ConfigListNode<T> listNode,
                                                                AbstractConfigNode parent,
                                                                ArrayNode arrayNode,
                                                                String password)
    throws ConfigurationException {
        setupNode(name, listNode, parent);
        if (arrayNode.size() > 0) {
            for (int ii = 0; ii < arrayNode.size(); ii++) {
                JsonNode nn = arrayNode.get(ii);
                String cname = String.valueOf(ii);
                parseNode(cname, nn, listNode, password);
            }
        }
    }


    /**
     * Setup the common node elements.
     *
     * @param name       - Node name.
     * @param configNode - Config node handle.
     * @param parent     - Parent config node.
     * @throws ConfigurationException
     */
    private void setupNode(String name, AbstractConfigNode configNode,
                           AbstractConfigNode parent)
    throws ConfigurationException {
        configNode.setName(name);
        configNode.setParent(parent);
        configNode.setConfiguration(configuration);
        configNode.loading();
        addToParentNode(parent, configNode);
    }

    /**
     * Setup the list node and add the list values.
     *
     * @param name       - Name of the List node.
     * @param parent     - Parent config node.
     * @param configNode - New list node to setup.
     * @param node       - JSON node to parse from.
     * @throws ConfigurationException
     */
    private void setupNodeWithChildren(String name, AbstractConfigNode parent,
                                       AbstractConfigNode configNode, JsonNode node,
                                       String password)
    throws ConfigurationException {
        setupNode(name, configNode, parent);
        parseChildNodes(configNode, node, password);
    }

    /**
     * Parse and add the child config nodes.
     *
     * @param parent - Config node to add children to.
     * @param node   - JSON node to read data from.
     * @throws ConfigurationException
     */
    private void parseChildNodes(AbstractConfigNode parent, JsonNode node,
                                 String password)
    throws ConfigurationException {
        Iterator<Map.Entry<String, JsonNode>> nnodes = node.fields();
        if (nnodes != null) {
            while (nnodes.hasNext()) {
                Map.Entry<String, JsonNode> sn = nnodes.next();
                if (isProcessed(sn.getValue())) {
                    continue;
                }
                parseNode(sn.getKey(), sn.getValue(), parent, password);
            }
        }
    }

    /**
     * Validate and add the specified node to the parent.
     *
     * @param parent - Parent configuration node.
     * @param node   - Child node to add.
     * @throws ConfigurationException
     */
    private void addToParentNode(AbstractConfigNode parent, AbstractConfigNode node)
    throws ConfigurationException {
        if (parent instanceof ConfigPathNode) {
            ((ConfigPathNode) parent).addChildNode(node);
        } else if (parent instanceof ConfigListElementNode) {
            if (node instanceof ConfigElementNode) {
                ((ConfigListElementNode) parent).addValue((ConfigElementNode) node);
            }
        } else {
            throw new ConfigurationException(String.format(
                    "Cannot add child node to parent node. [type=%s]",
                    parent.getClass().getCanonicalName()));
        }
    }

    /**
     * Read the JSON Object node and parse the config data.
     *
     * @param name   - Config Node name.
     * @param node   - JSON node to read data from.
     * @param parent - Parent config node.
     * @return - Parsed config node.
     * @throws ConfigurationException
     */
    private AbstractConfigNode readObjectNode(String name, JsonNode node,
                                              AbstractConfigNode parent,
                                              String password)
    throws ConfigurationException {
        AbstractConfigNode nn = null;
        if (name.compareTo(
                configuration.getSettings().getPropertiesNodeName()) == 0) {
            ConfigPropertiesNode pn =
                    new ConfigPropertiesNode(configuration, parent);
            setupNodeWithChildren(name, parent, pn, node, password);
            nn = pn;
        } else if (name.compareTo(
                configuration.getSettings().getParametersNodeName()) == 0) {
            ConfigParametersNode pn =
                    new ConfigParametersNode(configuration, parent);
            setupNodeWithChildren(name, parent, pn, node, password);
            nn = pn;
        } else if (name.compareTo(
                configuration.getSettings().getAttributesNodeName()) == 0) {
            ConfigAttributesNode pn =
                    new ConfigAttributesNode(configuration, parent);
            setupNodeWithChildren(name, parent, pn, node, password);
            nn = pn;
        } else if (name.compareTo(ConfigIncludeNode.NODE_NAME) == 0) {
            ConfigIncludeNode pn = new ConfigIncludeNode(configuration, parent);
            setupIncludeNode(name, pn, parent, node, password);
            nn = pn;
        } else if (name.compareTo(ConfigResourceNode.NODE_NAME) == 0) {
            EResourceType type = parseResourceType(node);
            if (type == null) {
                throw ConfigurationException.propertyNotFoundException(
                        ConfigResourceNode.NODE_RESOURCE_TYPE);
            }
            ConfigResourceFile pn = null;
            if (type == EResourceType.FILE) {
                pn = new ConfigResourceFile(configuration, parent);
                parseFileResourceNode(name, pn, parent, node);
            } else if (type == EResourceType.BLOB) {
                pn = new ConfigResourceBlob(configuration, parent);
                parseFileResourceNode(name, pn, parent, node);
            } else if (type == EResourceType.DIRECTORY) {
                pn = new ConfigResourceDirectory(configuration, parent);
                parseFolderResourceNode(name, (ConfigResourceDirectory) pn, parent,
                                        node);
            }
            nn = pn;
        } else {
            ConfigValueNode vn = checkEncryptedValue(name, parent, node);
            if (vn != null) {
                if (parent instanceof ConfigKeyValueNode) {
                    ((ConfigKeyValueNode) parent).addKeyValue(vn);
                } else {
                    if (parent instanceof ConfigPathNode) {
                        ((ConfigPathNode) parent).addChildNode(vn);
                    } else if (parent instanceof ConfigListValueNode) {
                        ((ConfigListValueNode) parent).addValue(vn);
                    } else {
                        throw new ConfigurationException(String.format(
                                "Cannot add string value to parent node. [type=%s]",
                                parent.getClass().getCanonicalName()));
                    }
                }
                nn = vn;
            } else {
                ConfigPathNode pn = new ConfigPathNode(configuration, parent);
                setupNodeWithChildren(name, parent, pn, node, password);
                nn = pn;
            }
        }
        return nn;
    }

    /**
     * Check if the current node is an encrypted Text node.
     *
     * @param name     - Node name.
     * @param parent   - Parent Config Node.
     * @param jsonNode - JSON Node element.
     * @return - Configuration Value Node.
     * @throws ConfigurationException
     */
    private ConfigValueNode checkEncryptedValue(String name,
                                                AbstractConfigNode parent,
                                                JsonNode jsonNode)
    throws ConfigurationException {
        JsonNode en = jsonNode.get(JSONConfigConstants.CONFIG_NODE_ENCRYPTED);
        if (en != null) {
            JsonNode vn =
                    jsonNode.get(JSONConfigConstants.CONFIG_NODE_ENCRYPTED_VALUE);
            if (vn == null) {
                throw new ConfigurationException(
                        "Invalid Encrypted node: Value is NULL.");
            }
            ConfigValueNode valueNode = new ConfigValueNode(configuration, parent);
            valueNode.setName(name);
            valueNode.setEncrypted(true);
            valueNode.setValue(vn.textValue());

            return valueNode;
        }
        return null;
    }

    /**
     * Parse a file resource node.
     *
     * @param name     - Node name.
     * @param node     - File Resource Config node.
     * @param parent   - Parent Config node.
     * @param jsonNode - JSON Node.
     * @throws ConfigurationException
     */
    private void parseFileResourceNode(String name, ConfigResourceFile node,
                                       AbstractConfigNode parent, JsonNode jsonNode)
    throws ConfigurationException {
        parseResourceNode(name, node, parent, jsonNode);
        URI uri = node.getLocation();
        if (uri == null) {
            throw ConfigurationException.propertyNotFoundException("location");
        }
        if (IOUtils.isLocalFile(uri)) {
            File file = Paths.get(uri).toFile();
            if (!file.exists()) {
                throw new ConfigurationException(String.format(
                        "Specified resource file not found : [path=%s]",
                        file.getAbsolutePath()));
            }
            node.setResourceHandle(file);
        } else {
            String filename = String.format("%s/%s", settings.getTempDirectory(),
                                            node.getResourceName());
            File file = new File(filename);
            IOUtils.CheckParentDirectory(file.getAbsolutePath());
            node.setResourceHandle(file);
            if (!file.exists()) {
                if (configuration.getSettings().getDownloadRemoteFiles() ==
                        ConfigurationSettings.EStartupOptions.OnStartUp) {
                    EReaderType type = EReaderType.parseFromUri(node.getLocation());
                    Preconditions.checkNotNull(type);

                    if (type == EReaderType.HTTP || type == EReaderType.HTTPS) {
                        try {
                            long bread = RemoteFileHelper
                                    .downloadRemoteFile(node.getLocation(),
                                                        node.getResourceHandle());
                            if (bread <= 0) {
                                throw new ConfigurationException(String.format(
                                        "No bytes read for remote file. [url=%s]",
                                        uri.toString()));
                            }
                            LogUtils.debug(getClass(), String.format(
                                    "Downloaded remote file. [path=%s][size=%s]",
                                    file.getAbsolutePath(), bread));
                        } catch (IOException e) {
                            throw new ConfigurationException(e);
                        }
                    }
                }
            }
        }
    }

    /**
     * Parse a file resource node.
     *
     * @param name     - Node name.
     * @param node     - Directory Resource Config node.
     * @param parent   - Parent Config node.
     * @param jsonNode - JSON Node.
     * @throws ConfigurationException
     */
    private void parseFolderResourceNode(String name, ConfigResourceDirectory node,
                                         AbstractConfigNode parent,
                                         JsonNode jsonNode)
    throws ConfigurationException {
        parseResourceNode(name, node, parent, jsonNode);
        URI uri = node.getLocation();
        if (uri == null) {
            throw ConfigurationException.propertyNotFoundException("location");
        }
        if (IOUtils.isLocalFile(uri)) {
            File file = Paths.get(uri).toFile();
            if (!file.exists()) {
                throw new ConfigurationException(String.format(
                        "Specified resource file not found : [path=%s]",
                        file.getAbsolutePath()));
            }
            node.setResourceHandle(file);
        } else {
            String filename = String.format("%s/%s", settings.getTempDirectory(),
                                            node.getResourceName());
            File file = new File(filename);
            IOUtils.CheckDirectory(file.getAbsolutePath());
            node.setResourceHandle(file);
            if (!file.exists()) {
                if (configuration.getSettings().getDownloadRemoteFiles() ==
                        ConfigurationSettings.EStartupOptions.OnStartUp) {
                    EReaderType type = EReaderType.parseFromUri(node.getLocation());
                    Preconditions.checkNotNull(type);

                    if (type == EReaderType.HTTP || type == EReaderType.HTTPS) {
                        try {
                            long bread = RemoteFileHelper
                                    .downloadRemoteDirectory(node.getLocation(),
                                                             node.getResourceHandle());
                            if (bread <= 0) {
                                throw new ConfigurationException(String.format(
                                        "No bytes read for remote file. [url=%s]",
                                        uri.toString()));
                            }
                        } catch (IOException e) {
                            throw new ConfigurationException(e);
                        }
                    }
                }
            }
        }
    }

    /**
     * Extract and parse the resource node type.
     *
     * @param node - JsonNode to extract from.
     * @return - Parsed Resource node type.
     * @throws ConfigurationException
     */
    private EResourceType parseResourceType(JsonNode node)
    throws ConfigurationException {
        JsonNode nn = node.get(ConfigResourceNode.NODE_RESOURCE_TYPE);
        if (nn == null) {
            throw ConfigurationException.propertyNotFoundException(
                    ConfigResourceNode.NODE_RESOURCE_TYPE);
        }
        String nt = nn.textValue();
        if (Strings.isNullOrEmpty(nt)) {
            throw ConfigurationException.propertyNotFoundException(
                    ConfigResourceNode.NODE_RESOURCE_TYPE);
        }
        return EResourceType.valueOf(nt);
    }

    /**
     * Parse the Resource node parameters.
     *
     * @param name     - Node name.
     * @param node     - Resource node handle.
     * @param parent   - Parent Config node.
     * @param jsonNode - JSON node.
     * @throws ConfigurationException
     */
    private void parseResourceNode(String name,
                                   ConfigResourceNode node,
                                   AbstractConfigNode parent,
                                   JsonNode jsonNode)
    throws ConfigurationException {
        setupNode(name, node, parent);
        Iterator<Map.Entry<String, JsonNode>> nnodes = jsonNode.fields();
        if (nnodes != null) {
            while (nnodes.hasNext()) {
                Map.Entry<String, JsonNode> sn = nnodes.next();
                if (isProcessed(sn.getValue())) {
                    continue;
                }
                String nname = sn.getKey();
                JsonNode cnode = sn.getValue();

                if (nname.compareTo(ConfigResourceNode.NODE_RESOURCE_TYPE) == 0) {
                    if (cnode.getNodeType() == JsonNodeType.STRING) {
                        String nt = cnode.textValue();
                        if (!Strings.isNullOrEmpty(nt)) {
                            EResourceType type = EResourceType.valueOf(nt);
                            if (type == null) {
                                throw new ConfigurationException(String.format(
                                        "Invalid Resource Type : [type=%s]", nt));
                            }
                            node.setType(type);
                        }
                    }
                } else if (nname.compareTo(ConfigResourceNode.NODE_RESOURCE_URL) ==
                        0) {
                    if (cnode.getNodeType() == JsonNodeType.STRING) {
                        String loc = cnode.textValue();
                        if (!Strings.isNullOrEmpty(loc)) {
                            try {
                                URI uri = new URI(loc);
                                node.setLocation(uri);
                            } catch (URISyntaxException e) {
                                throw new ConfigurationException(e);
                            }
                        }
                    }
                } else if (nname.compareTo(ConfigResourceNode.NODE_RESOURCE_NAME) ==
                        0) {
                    if (cnode.getNodeType() == JsonNodeType.STRING) {
                        String rname = cnode.textValue();
                        if (!Strings.isNullOrEmpty(rname)) {
                            node.setResourceName(rname);
                        }
                    }
                }
            }
        }
    }

    /**
     * Setup the configuration node for a config include. This will create the
     * new configuration instance and attach it to this node.
     *
     * @param name     - Node Name.
     * @param node     - Config Include node handle.
     * @param parent   - Parent config node.
     * @param jsonNode - JSON Node.
     * @throws ConfigurationException
     */
    private void setupIncludeNode(String name, ConfigIncludeNode node,
                                  AbstractConfigNode parent,
                                  JsonNode jsonNode, String password)
    throws ConfigurationException {
        setupNode(name, node, parent);
        Iterator<Map.Entry<String, JsonNode>> nnodes = jsonNode.fields();
        if (nnodes != null) {
            while (nnodes.hasNext()) {
                Map.Entry<String, JsonNode> sn = nnodes.next();
                if (isProcessed(sn.getValue())) {
                    continue;
                }
                String nname = sn.getKey();
                JsonNode cnode = sn.getValue();
                if (nname.compareTo(ConfigIncludeNode.NODE_TYPE) == 0) {
                    if (cnode.getNodeType() == JsonNodeType.STRING) {
                        String nt = cnode.textValue();
                        if (!Strings.isNullOrEmpty(nt)) {
                            EReaderType rt = EReaderType.parse(nt);
                            node.setReaderType(rt);
                        } else {
                            throw new ConfigurationException(
                                    "Invalid node value : Value is NULL/Empty");
                        }
                    } else {
                        throw new ConfigurationException(String.format(
                                "Invalid node type : [expected=%s][actual=%s]",
                                JsonNodeType.STRING.name(),
                                cnode.getNodeType().name()));
                    }
                } else if (nname.compareToIgnoreCase(ConfigIncludeNode.NODE_PATH) ==
                        0) {
                    if (cnode.getNodeType() == JsonNodeType.STRING) {
                        String path = cnode.textValue();
                        if (!Strings.isNullOrEmpty(path)) {
                            node.setPath(path);
                        } else {
                            throw new ConfigurationException(
                                    "Invalid node value : Value is NULL/Empty");
                        }
                    } else {
                        throw new ConfigurationException(String.format(
                                "Invalid node type : [expected=%s][actual=%s]",
                                JsonNodeType.STRING.name(),
                                cnode.getNodeType().name()));
                    }
                } else if (
                        nname.compareToIgnoreCase(ConfigIncludeNode.NODE_VERSION) ==
                                0) {
                    if (cnode.getNodeType() == JsonNodeType.STRING) {
                        String version = cnode.textValue();
                        if (!Strings.isNullOrEmpty(version)) {
                            try {
                                node.setVersion(Version.parse(version));
                            } catch (ValueParseException e) {
                                throw new ConfigurationException(e);
                            }
                        } else {
                            throw new ConfigurationException(
                                    "Invalid node value : Value is NULL/Empty");
                        }
                    } else {
                        throw new ConfigurationException(String.format(
                                "Invalid node type : [expected=%s][actual=%s]",
                                JsonNodeType.STRING.name(),
                                cnode.getNodeType().name()));
                    }
                } else if (
                        nname.compareToIgnoreCase(
                                ConfigIncludeNode.NODE_CONFIG_NAME) ==
                                0) {
                    if (cnode.getNodeType() == JsonNodeType.STRING) {
                        String configName = cnode.textValue();
                        if (!Strings.isNullOrEmpty(configName)) {
                            node.setConfigName(configName);
                        } else {
                            throw new ConfigurationException(
                                    "Invalid node value : Value is NULL/Empty");
                        }
                    } else {
                        throw new ConfigurationException(String.format(
                                "Invalid node type : [expected=%s][actual=%s]",
                                JsonNodeType.STRING.name(),
                                cnode.getNodeType().name()));
                    }
                }
            }
            URI uri = node.getURI();
            if (uri == null) {
                throw new ConfigurationException(
                        "Error getting URI for include node.");
            }
            AbstractConfigReader reader = ConfigProviderFactory.reader(uri);
            if (reader == null) {
                throw new ConfigurationException(
                        String.format("Error getting reader instance : [URI=%s]",
                                      uri.toString()));
            }
            JSONConfigParser nparser = new JSONConfigParser();
            nparser.parse(node.getConfigName(), reader, settings,
                          node.getVersion(), password);
            if (nparser.configuration != null) {
                ConfigPathNode configPathNode =
                        nparser.configuration.getRootConfigNode();
                if (parent instanceof ConfigPathNode) {
                    ((ConfigPathNode) parent).addChildNode(configPathNode);
                    configPathNode.changeConfiguration(configuration);
                    node.setNode(configPathNode);
                } else {
                    throw new ConfigurationException(String.format(
                            "Error adding include node : [expected parent=%s][actual parent=%s]",
                            ConfigPathNode.class.getCanonicalName(),
                            parent.getClass().getCanonicalName()));
                }
            } else {
                throw new ConfigurationException(String.format(
                        "Error loading included configuration. [URI=%s]",
                        uri.toString()));
            }
        } else {
            throw new ConfigurationException(
                    "Invalid include definition : No include configuration specified.");
        }
    }

    /**
     * Read the configuration header information.
     * <p>
     * Header:
     * - Configuration Name
     * - Configuration Version
     * - Created By
     * - Updated By
     *
     * @param node    - Root node to find header under.
     * @param version - Expected Compatibility Version.
     * @throws ConfigurationException
     */
    private void parseHeader(JsonNode node, Version version, String password)
    throws ConfigurationException {
        try {
            JsonNode header = node.get(JSONConfigConstants.CONFIG_HEADER_NODE);
            if (header == null) {
                throw ConfigurationException
                        .propertyNotFoundException(
                                JSONConfigConstants.CONFIG_HEADER_NODE);
            }
            if (!isProcessed(header)) {
                // Read the configuration ID
                JsonNode hid = header.get(JSONConfigConstants.CONFIG_HEADER_ID);
                if (hid == null) {
                    throw ConfigurationException
                            .propertyNotFoundException(
                                    JSONConfigConstants.CONFIG_HEADER_ID);
                }
                String id = hid.textValue();
                Preconditions.checkState(!Strings.isNullOrEmpty(id));
                configuration.setId(id);
                // Read the Application Group.
                JsonNode hgrp = header.get(JSONConfigConstants.CONFIG_HEADER_GROUP);
                if (hgrp == null) {
                    throw ConfigurationException
                            .propertyNotFoundException(
                                    JSONConfigConstants.CONFIG_HEADER_GROUP);
                }
                String grp = hgrp.textValue();
                Preconditions.checkState(!Strings.isNullOrEmpty(grp));
                configuration.setApplicationGroup(grp);
                // Read the Application.
                JsonNode happ = header.get(JSONConfigConstants.CONFIG_HEADER_APP);
                if (happ == null) {
                    throw ConfigurationException
                            .propertyNotFoundException(
                                    JSONConfigConstants.CONFIG_HEADER_APP);
                }
                String app = happ.textValue();
                Preconditions.checkState(!Strings.isNullOrEmpty(app));
                configuration.setApplication(app);

                // Read the configuration name
                JsonNode hname = header.get(JSONConfigConstants.CONFIG_HEADER_NAME);
                if (hname == null) {
                    throw ConfigurationException
                            .propertyNotFoundException(
                                    JSONConfigConstants.CONFIG_HEADER_NAME);
                }
                String sname = hname.textValue();
                // Configuration name in resource should match the expected configuration name.
                if (configuration.getName().compareTo(sname) != 0) {
                    throw new ConfigurationException(String.format(
                            "Invalid configuration : Name does not match. [expected=%s][actual=%s]",
                            configuration.getName(), sname));
                }
                // Read the configuration version.
                JsonNode vnode =
                        header.get(JSONConfigConstants.CONFIG_HEADER_VERSION);
                if (vnode == null) {
                    throw ConfigurationException
                            .propertyNotFoundException(
                                    JSONConfigConstants.CONFIG_HEADER_VERSION);
                }
                String vstring = vnode.textValue();
                Version cversion = Version.parse(vstring);
                // Check version compatibility
                if (!version.isCompatible(cversion)) {
                    throw new ConfigurationException(String.format(
                            "Incompatible Configuration Version. [expected=%s][actual=%s]",
                            version.toString(), cversion.toString()));
                }
                configuration.setVersion(cversion);

                // Read the configuration creation info.
                JsonNode cnode = header.get(JSONConfigConstants.CONFIG_CREATED_BY);
                if (cnode == null) {
                    throw ConfigurationException
                            .propertyNotFoundException(
                                    JSONConfigConstants.CONFIG_CREATED_BY);
                }
                ModifiedBy createdBy = parseUpdateInfo(cnode);
                configuration.setCreatedBy(createdBy);

                // Read the configuration Last updation info.
                JsonNode unode = header.get(JSONConfigConstants.CONFIG_UPDATED_BY);
                if (unode == null) {
                    throw ConfigurationException
                            .propertyNotFoundException(
                                    JSONConfigConstants.CONFIG_UPDATED_BY);
                }
                ModifiedBy updatedBy = parseUpdateInfo(unode);
                configuration.setUpdatedBy(updatedBy);

                JsonNode dnode = header.get(JSONConfigConstants.CONFIG_HEADER_DESC);
                if (dnode != null) {
                    String desc = dnode.textValue();
                    if (!Strings.isNullOrEmpty(desc)) {
                        configuration.setDescription(desc);
                    }
                }
                // Check if an encryption hash is specified.
                JsonNode phnode =
                        header.get(JSONConfigConstants.CONFIG_HEADER_PASSWD_HASH);
                if (phnode != null) {
                    String hash = phnode.textValue();
                    if (Strings.isNullOrEmpty(hash)) {
                        throw new ConfigurationException(
                                "Invalid Password Hash: NULL or Empty.");
                    }
                    if (Strings.isNullOrEmpty(password)) {
                        throw new ConfigurationException(String.format(
                                "Configuration has encryption, but no passcode specified. [config=%s]",
                                configuration.getName()));
                    }
                    String chash = CypherUtils.getKeyHash(password);
                    if (hash.compareTo(chash) != 0) {
                        throw new ConfigurationException(String.format(
                                "Invalid Passcode: Doesn't match with passcode set in configuration. [config=%s]",
                                configuration.getName()));
                    }
                    configuration.setEncryptionHash(hash);
                }
            }
        } catch (ConfigurationException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Read the modification information from the specified JSON node.
     *
     * @param node - JSON node to read info under.
     * @return - Modification info object.
     * @throws ConfigurationException
     */
    private ModifiedBy parseUpdateInfo(JsonNode node)
    throws ConfigurationException {
        if (isProcessed(node)) {
            return null;
        }

        JsonNode jn = node.get(JSONConfigConstants.CONFIG_UPDATE_OWNER);
        if (jn == null) {
            throw ConfigurationException
                    .propertyNotFoundException(
                            JSONConfigConstants.CONFIG_UPDATE_OWNER);
        }
        String owner = jn.textValue();
        if (Strings.isNullOrEmpty(owner)) {
            throw new ConfigurationException(
                    "Invalid Configuration : Update Owner is NULL/Empty.");
        }
        jn = node.get(JSONConfigConstants.CONFIG_UPDATE_TIMESTAMP);
        if (jn == null) {
            throw ConfigurationException
                    .propertyNotFoundException(
                            JSONConfigConstants.CONFIG_UPDATE_TIMESTAMP);
        }
        String timestamp = jn.textValue();
        if (Strings.isNullOrEmpty(timestamp)) {
            throw new ConfigurationException(
                    "Invalid Configuration : Update Timestamp is NULL/Empty.");
        }

        long dt = Long.parseLong(timestamp);
        ModifiedBy modifiedBy = new ModifiedBy();
        modifiedBy.setModifiedBy(owner);
        modifiedBy.setTimestamp(dt);

        return modifiedBy;
    }

    @Override
    public void close() throws IOException {

    }
}
