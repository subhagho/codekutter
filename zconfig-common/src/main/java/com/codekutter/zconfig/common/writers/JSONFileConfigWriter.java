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
 * Date: 4/1/19 5:47 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.writers;

import com.codekutter.common.model.ModifiedBy;
import com.codekutter.common.utils.DateTimeUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.JSONConfigConstants;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.nodes.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Configuration writer implementation that writes the specified configuration to JSON format output.
 */
public class JSONFileConfigWriter extends AbstractConfigWriter {
    /**
     * JSON Object Mapper instance.
     */
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * Write the configuration as a JSON to the specified output location.
     * The name of the generated output file will be of the following format [config_name]_[instance_id].json.
     *
     * @param configuration - Configuration handle to serialize.
     * @param path          - Output location to write to.
     * @return - Return the path of the output file created.
     * @throws ConfigurationException
     */
    @Override
    public String write(Configuration configuration, String path)
    throws ConfigurationException {
        Preconditions.checkArgument(configuration != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));

        File outdir = new File(path);
        if (!outdir.exists() || !outdir.isDirectory()) {
            throw new ConfigurationException("Invalid output directory specified : Directory does not exist or path isn't a directory.");
        }
        File outfile = new File(
                String.format("%s/%s_%s.json", outdir.getAbsolutePath(),
                              configuration.getName(),
                              configuration.getInstanceId()));
        if (outfile.exists()) {
            if (!outfile.delete()) {
                throw new ConfigurationException(
                        String.format("Error removing existing file. [file=%s]",
                                      outfile.getAbsolutePath()));
            }
        }
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.registerModule(new JodaModule());

        serializeToJson(configuration, outfile);

        return outfile.getAbsolutePath();
    }

    /**
     * Serialize the configuration to the specified output file.
     *
     * @param configuration - Configuration to serialize.
     * @param outfile       - Output file handle.
     * @throws ConfigurationException
     */
    private void serializeToJson(Configuration configuration, File outfile)
    throws ConfigurationException {
        JsonNode rootNode = mapper.createObjectNode();
        addConfigHeader(configuration, (ObjectNode) rootNode);
        addConfigBody(configuration, (ObjectNode) rootNode);

        // Write the generated JSON to the output file.
        writeJSON(rootNode, outfile);
    }

    /**
     * Write the generated JSON tree to the specified output file.
     *
     * @param rootNode - JSON Tree root node.
     * @param outfile  - Output file handle.
     * @throws ConfigurationException
     */
    private void writeJSON(JsonNode rootNode, File outfile)
    throws ConfigurationException {
        try {
            String json = mapper.writeValueAsString(rootNode);
            try (FileOutputStream fos = new FileOutputStream(outfile)) {
                fos.write(json.getBytes());
                fos.flush();
            }
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Add the Configuration Body to the JSON Tree.
     *
     * @param configuration - Configuration instance.
     * @param parent        - Parent JSON node.
     * @throws ConfigurationException
     */
    private void addConfigBody(Configuration configuration, ObjectNode parent)
    throws ConfigurationException {
        ConfigPathNode rootNode = configuration.getRootConfigNode();
        addConfigPathNode(rootNode, parent);
    }

    /**
     * Process the configuration node, based on the node type will call the appropriate method.
     *
     * @param node           - Configuration node to process.
     * @param parentJsonNode - Parent JSON node.
     * @throws ConfigurationException
     */
    private void addConfigNode(AbstractConfigNode node,
                               JsonNode parentJsonNode)
    throws ConfigurationException {
        if (node instanceof ConfigPathNode) {
            if (parentJsonNode instanceof ObjectNode) {
                addConfigPathNode((ConfigPathNode) node,
                                  (ObjectNode) parentJsonNode);
            } else if (parentJsonNode instanceof ArrayNode) {
                addConfigPathNode((ConfigPathNode) node,
                                  (ArrayNode) parentJsonNode);
            }
        } else if (node instanceof ConfigListElementNode &&
                (parentJsonNode instanceof ObjectNode)) {
            addListNode((ConfigListElementNode) node, (ObjectNode) parentJsonNode);
        } else if (node instanceof ConfigValueNode) {
            addConfigValue((ConfigValueNode) node, parentJsonNode);
        } else if (node instanceof ConfigKeyValueNode &&
                (parentJsonNode instanceof ObjectNode)) {
            addKeyValueNode((ConfigKeyValueNode) node, (ObjectNode) parentJsonNode);
        } else if (node instanceof ConfigListValueNode &&
                (parentJsonNode instanceof ObjectNode)) {
            ArrayNode valueList =
                    ((ObjectNode) parentJsonNode).putArray(node.getName());
            List<ConfigValueNode> values = ((ConfigListValueNode) node).getValues();
            if (values != null && !values.isEmpty()) {
                for (ConfigValueNode cv : values) {
                    addConfigValue(cv, valueList);
                }
            }
        }
    }

    /**
     * Add a Key/Value node (Parameters/Properties) to the parent Object Node.
     *
     * @param node   - Configuration Key/Value set node.
     * @param parent - Parent JSON node.
     * @throws ConfigurationException
     */
    private void addKeyValueNode(ConfigKeyValueNode node, ObjectNode parent)
    throws ConfigurationException {
        if (Strings.isNullOrEmpty(node.getName())) {
            throw ConfigurationException.propertyNotFoundException(
                    JSONConfigConstants.CONFIG_HEADER_NAME);
        }
        ObjectNode cnode = parent.putObject(node.getName());
        Map<String, ConfigValueNode> keyValues = node.getKeyValues();
        if (keyValues != null && !keyValues.isEmpty()) {
            for (String key : keyValues.keySet()) {
                cnode.put(key, keyValues.get(key).getValue());
            }
        }
    }

    /**
     * Add a new Object node based on the configuration path element.
     *
     * @param node   - Configuration Path node.
     * @param parent - Parent JSON node.
     * @throws ConfigurationException
     */
    private void addConfigPathNode(ConfigPathNode node, ObjectNode parent)
    throws ConfigurationException {
        if (Strings.isNullOrEmpty(node.getName())) {
            throw ConfigurationException.propertyNotFoundException(
                    JSONConfigConstants.CONFIG_HEADER_NAME);
        }
        ObjectNode cnode = parent.putObject(node.getName());

        if (node.getChildren() != null) {
            Map<String, AbstractConfigNode> nodes = node.getChildren();
            if (!nodes.isEmpty()) {
                for (String name : nodes.keySet()) {
                    addConfigNode(nodes.get(name), cnode);
                }
            }
        }
    }

    private void addConfigPathNode(ConfigPathNode node, ArrayNode parent)
    throws ConfigurationException {
        if (Strings.isNullOrEmpty(node.getName())) {
            throw ConfigurationException.propertyNotFoundException(
                    JSONConfigConstants.CONFIG_HEADER_NAME);
        }
        ObjectNode onode = parent.addObject();
        if (node.getChildren() != null) {
            Map<String, AbstractConfigNode> nodes = node.getChildren();
            if (!nodes.isEmpty()) {
                for (String name : nodes.keySet()) {
                    addConfigNode(nodes.get(name), onode);
                }
            }
        }
    }

    /**
     * Add the List (Array) node and the List elements.
     *
     * @param node   - Element List Node.
     * @param parent - Parent JSON node.
     * @throws ConfigurationException
     */
    private void addListNode(ConfigListElementNode node,
                             ObjectNode parent) throws ConfigurationException {
        if (Strings.isNullOrEmpty(node.getName())) {
            throw ConfigurationException.propertyNotFoundException(
                    JSONConfigConstants.CONFIG_HEADER_NAME);
        }
        ArrayNode array = parent.putArray(node.getName());
        List<ConfigElementNode> values = node.getValues();
        if (values != null && !values.isEmpty()) {
            for (ConfigElementNode vn : values) {
                addConfigNode(vn, array);
            }
        }
    }

    /**
     * Add a configuration value node to the parent node passed.
     *
     * @param value - Configuration value node.
     * @param node  - Parent JSON node.
     * @throws ConfigurationException
     */
    private void addConfigValue(ConfigValueNode value, JsonNode node)
    throws ConfigurationException {
        if (node.getNodeType() == JsonNodeType.ARRAY) {
            ArrayNode array = (ArrayNode) node;
            array.add(value.getValue());
        } else if (node.getNodeType() == JsonNodeType.OBJECT) {
            ObjectNode onode = (ObjectNode) node;
            onode.put(value.getName(), value.getValue());
        } else {
            throw new ConfigurationException(String.format(
                    "Cannot add node to parent. [config node=%s][parent=%s]",
                    value.getClass().getCanonicalName(),
                    node.getNodeType().name()));
        }
    }

    /**
     * Add the configuration header node.
     *
     * @param configuration - Configuration instance.
     * @param node          - Root node to add the header to.
     * @throws ConfigurationException
     */
    private void addConfigHeader(Configuration configuration, ObjectNode node)
    throws ConfigurationException {
        ObjectNode header = node.putObject(JSONConfigConstants.CONFIG_HEADER_NODE);
        if (Strings.isNullOrEmpty(configuration.getName())) {
            throw ConfigurationException.propertyNotFoundException(
                    JSONConfigConstants.CONFIG_HEADER_NAME);
        }
        header.put(JSONConfigConstants.CONFIG_HEADER_NAME, configuration.getName());
        if (configuration.getVersion() == null) {
            throw ConfigurationException.propertyNotFoundException(
                    JSONConfigConstants.CONFIG_HEADER_VERSION);
        }
        header.put(JSONConfigConstants.CONFIG_HEADER_VERSION,
                   configuration.getVersion().toString());
        addUpdateInfo(configuration.getCreatedBy(),
                      JSONConfigConstants.CONFIG_CREATED_BY, header);
        addUpdateInfo(configuration.getUpdatedBy(),
                      JSONConfigConstants.CONFIG_UPDATED_BY, header);
        if (!Strings.isNullOrEmpty(configuration.getDescription())) {
            header.put(JSONConfigConstants.CONFIG_HEADER_DESC,
                       configuration.getDescription());
        }
    }

    /**
     * Added the Updation Info to the parent node specified.
     *
     * @param updateInfo - Updation Information
     * @param name       - Name of the node to create.
     * @param parent     - Parent node to add to.
     * @throws ConfigurationException
     */
    private void addUpdateInfo(ModifiedBy updateInfo, String name,
                               ObjectNode parent) throws ConfigurationException {
        if (updateInfo == null) {
            throw ConfigurationException.propertyNotFoundException(name);
        }
        ObjectNode node = parent.putObject(name);
        if (Strings.isNullOrEmpty(updateInfo.getModifiedBy())) {
            throw ConfigurationException.propertyNotFoundException(
                    JSONConfigConstants.CONFIG_UPDATE_OWNER);
        }
        node.put(JSONConfigConstants.CONFIG_UPDATE_OWNER,
                 updateInfo.getModifiedBy());

        node.put(JSONConfigConstants.CONFIG_UPDATE_TIMESTAMP,
                 DateTimeUtils.toString(updateInfo.getTimestamp()));
    }
}
