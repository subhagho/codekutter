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
 * Date: 2/1/19 9:56 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.parsers;

import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.VariableRegexParser;
import com.codekutter.zconfig.common.model.*;
import com.codekutter.zconfig.common.model.nodes.*;
import com.codekutter.zconfig.common.readers.AbstractConfigReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.hibernate.Session;

import javax.annotation.Nonnull;
import javax.persistence.Query;
import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for defining configuration parsers.
 */
public abstract class AbstractConfigParser implements Closeable {

    /**
     * Configuration settings to be used by this parser.
     */
    protected ConfigurationSettings settings = null;

    /**
     * Configuration instance handle.
     */
    protected Configuration configuration;

    /**
     * Get the handle to the parsed configuration.
     *
     * @return - Configuration instance.
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Method to be called post loading of the configuration.
     * <p>
     * This method updates property values if required.
     *
     * @throws ConfigurationException
     */
    protected void doPostLoad() throws ConfigurationException {
        ConfigPathNode node = configuration.getRootConfigNode();
        if (node != null) {
            try {
                AbstractConfigNode cnode =
                        ConfigUtils.getPathNode(AbstractConnection.class, node);
                if (cnode != null) {
                    AbstractConnection<Session> connection = readConnection(node);
                    if (connection != null) {
                        Map<String, ConfigDbRecord> records = fetch(connection);
                        if (records != null) {
                            readConfigFromDb(node, records);
                        }
                    }
                }
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
            Map<String, ConfigValueNode> properties = new HashMap<>();
            nodePostLoad(node, properties);
        }
        // Validate the configuration
        configuration.getRootConfigNode().validate();
        // Mark the configuration has been loaded.
        configuration.loaded();
    }

    /**
     * Replace variable values with the scoped property sets.
     *
     * @param node       - Node to preform replacement on.
     * @param inputProps - Input Property set.
     * @throws ConfigurationException
     */
    private void nodePostLoad(AbstractConfigNode node,
                              Map<String, ConfigValueNode> inputProps) throws ConfigurationException {
        Map<String, ConfigValueNode> properties = new HashMap<>(inputProps);
        if (node instanceof ConfigPathNode) {
            // Get defined properties, if any.
            ConfigPathNode cp = (ConfigPathNode) node;
            ConfigPropertiesNode props = cp.properties();
            if (props != null) {
                Map<String, ConfigValueNode> pp = props.getKeyValues();
                if (pp != null && !pp.isEmpty()) {
                    properties.putAll(pp);
                }
            }

            // Do property replacement for all child nodes.
            Map<String, AbstractConfigNode> nodes = cp.getChildren();
            if (nodes != null && !nodes.isEmpty()) {
                for (String key : nodes.keySet()) {
                    nodePostLoad(nodes.get(key), properties);
                }
            }
        } else if (node instanceof ConfigKeyValueNode) {
            // Check parameter value replacement.
            ConfigKeyValueNode params = (ConfigKeyValueNode) node;
            Map<String, ConfigValueNode> pp = params.getKeyValues();
            for (String key : pp.keySet()) {
                String value = pp.get(key).getValue();
                if (!Strings.isNullOrEmpty(value)) {
                    String nValue = replaceVariables(value, properties);
                    if (value.compareTo(nValue) != 0) {
                        params.addKeyValue(key, nValue);
                    }
                }
            }
        } else if (node instanceof ConfigListElementNode) {
            ConfigListElementNode le = (ConfigListElementNode) node;
            List<ConfigElementNode> nodes = le.getValues();
            if (nodes != null && !nodes.isEmpty()) {
                for (ConfigElementNode nn : nodes) {
                    nodePostLoad(nn, properties);
                }
            }
        } else if (node instanceof ConfigListValueNode) {
            ConfigListValueNode le = (ConfigListValueNode) node;
            List<ConfigValueNode> nodes = le.getValues();
            if (nodes != null && !nodes.isEmpty()) {
                for (ConfigValueNode nn : nodes) {
                    nodePostLoad(nn, properties);
                }
            }
        } else if (node instanceof ConfigValueNode) {
            ConfigValueNode cv = (ConfigValueNode) node;
            String value = cv.getValue();
            if (!Strings.isNullOrEmpty(value)) {
                String nValue = replaceVariables(value, properties);
                if (value.compareTo(nValue) != 0) {
                    cv.setValue(nValue);
                }
            }
        }
    }

    /**
     * Replace all the variables, if any, defined in the value string with the property values specified.
     *
     * @param value      - Value String to replace variables in.
     * @param properties - Property Map to lookup variable values.
     * @return - Replaced String
     */
    private String replaceVariables(String value, Map<String, ConfigValueNode> properties) {
        if (!Strings.isNullOrEmpty(value)) {
            if (VariableRegexParser.hasVariable(value)) {
                List<String> vars = VariableRegexParser.getVariables(value);
                if (vars != null && !vars.isEmpty()) {
                    for (String var : vars) {
                        ConfigValueNode vn = properties.get(var);
                        if (vn != null) {
                            String vv = vn.getValue();
                            if (!Strings.isNullOrEmpty(vv)) {
                                String rv = String.format("\\$\\{%s\\}", var);
                                value = value.replaceAll(rv, vv);
                            }
                        } else {
                            String vv = System.getProperty(var);
                            if (Strings.isNullOrEmpty(vv)) {
                                vv = System.getenv(var);
                            }
                            if (!Strings.isNullOrEmpty(vv)) {
                                String rv = String.format("\\$\\{%s\\}", var);
                                value = value.replaceAll(rv, vv);
                            }
                        }
                    }
                }
            }
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, ConfigDbRecord> fetch(AbstractConnection<Session> connection) throws ConfigurationException {
        try {
            String qstr = String.format(
                    "FROM %s WHERE configId = :config AND majorVersion = :version",
                    ConfigDbRecord.class.getCanonicalName());
            Session session = connection.connection();
            try {
                Query query = session.createQuery(qstr);
                query.setParameter("config", configuration.getId());
                query.setParameter("version",
                        configuration.getVersion().getMajorVersion());
                List<ConfigDbRecord> records = query.getResultList();
                if (records != null && !records.isEmpty()) {
                    Map<String, ConfigDbRecord> map = new HashMap<>();
                    for (ConfigDbRecord record : records) {
                        String key =
                                String.format("%s/%s", record.getId().getPath(),
                                        record.getId().getName());
                        map.put(key, record);
                    }
                    return map;
                }
            } finally {
                session.close();
            }
            return null;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    protected AbstractConnection<Session> readConnection(@Nonnull ConfigPathNode node) throws ConfigurationException {
        AbstractConnection<Session> connection =
                ConnectionManager.readConnection(node);
        LogUtils.debug(getClass(),
                String.format("Loaded Db connection: [name=%s][type=%s]",
                        connection.name(),
                        connection.getClass().getCanonicalName()));
        configuration.setConnection(connection);

        return connection;
    }

    private void readConfigFromDb(ConfigPathNode node,
                                  Map<String, ConfigDbRecord> records) throws ConfigurationException {
        Preconditions.checkState(configuration.getConnection() != null);

        if (node instanceof ConfigDbNode) {
            if (node.attributes() != null) {
                Map<String, ConfigValueNode> attrs =
                        node.attributes().getKeyValues();
                if (!attrs.isEmpty()) {
                    for (String key : attrs.keySet()) {
                        ConfigValueNode vn = attrs.get(key);
                        if (vn.getNodeSource() == ENodeSource.DataBase) {
                            String path = vn.getAbsolutePath();
                            if (records.containsKey(path)) {
                                ConfigDbRecord record = records.get(path);
                                vn.setValueType(record.getValueType());
                                vn.setValue(record.getValue());
                            }
                        }
                    }
                }
            }
            if (node.parmeters() != null) {
                Map<String, ConfigValueNode> params =
                        node.parmeters().getKeyValues();
                if (!params.isEmpty()) {
                    for (String key : params.keySet()) {
                        ConfigValueNode vn = params.get(key);
                        if (vn.getNodeSource() == ENodeSource.DataBase) {
                            String path = vn.getAbsolutePath();
                            if (records.containsKey(path)) {
                                ConfigDbRecord record = records.get(path);
                                vn.setValueType(record.getValueType());
                                vn.setValue(record.getValue());
                                vn.setEncrypted(record.isEncrypted());
                            }
                        }
                    }
                }
            }
            if (node.getChildren() != null && !node.getChildren().isEmpty()) {
                Map<String, AbstractConfigNode> children = node.getChildren();
                for (String key : children.keySet()) {
                    AbstractConfigNode cnode = children.get(key);
                    if (cnode instanceof ConfigPathNode) {
                        readConfigFromDb((ConfigPathNode) cnode, records);
                    } else if (cnode instanceof ConfigValueNode) {
                        ConfigValueNode vn = (ConfigValueNode) cnode;
                        if (vn.getNodeSource() == ENodeSource.DataBase) {
                            if (records.containsKey(cnode.getAbsolutePath())) {
                                ConfigDbRecord record =
                                        records.get(vn.getAbsolutePath());
                                vn.setValueType(record.getValueType());
                                vn.setValue(record.getValue());
                                vn.setEncrypted(record.isEncrypted());
                            }
                        }
                    } else if (cnode instanceof ConfigListValueNode) {
                        int count = 0;
                        List<ConfigValueNode> vns =
                                ((ConfigListValueNode) cnode).getValues();
                        String path = vns.get(0).getAbsolutePath();
                        while (true) {
                            String k = String.format("%s/%d", path, count);
                            if (!records.containsKey(k)) break;
                            ConfigDbRecord record = records.get(k);
                            if (count >= vns.size()) {
                                ConfigValueNode vn = new ConfigValueNode(
                                        cnode.getConfiguration(), cnode);
                                vn.setName(record.getId().getName());
                                vn.setValue(record.getValue());
                                vn.setEncrypted(record.isEncrypted());
                                ((ConfigListValueNode) cnode).addValue(vn);
                            } else {
                                ConfigValueNode vn = vns.get(count);
                                vn.setName(record.getId().getName());
                                vn.setValue(record.getValue());
                                vn.setEncrypted(record.isEncrypted());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Parse and load the configuration instance using the specified properties.
     *
     * @param name     - Configuration name being loaded.
     * @param reader   - Configuration reader handle to read input from.
     * @param settings - Configuration Settings to use for parsing.
     * @param version  - Configuration version to load.
     * @param password - Password in case the Configuration has encrypted elements.
     * @throws ConfigurationException
     */
    public abstract void parse(String name, AbstractConfigReader reader,
                               ConfigurationSettings settings,
                               Version version, String password)
            throws ConfigurationException;

}
