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
 * Date: 5/1/19 6:20 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.model;

import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigProviderFactory;
import com.codekutter.zconfig.common.ConfigTestConstants;
import com.codekutter.zconfig.common.model.nodes.*;
import com.codekutter.zconfig.common.parsers.JSONConfigParser;
import com.codekutter.zconfig.common.readers.ConfigFileReader;
import com.google.common.base.Strings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.util.Properties;

import static com.codekutter.common.utils.LogUtils.*;
import static org.junit.jupiter.api.Assertions.*;

class Test_JSONConfiguration {
    private static final String BASE_PROPS_FILE =
            "src/test/resources/json/test-config.properties";
    private static Configuration configuration = null;

    @BeforeAll
    static void init() throws Exception {
        JSONConfigParser parser =
                (JSONConfigParser) ConfigProviderFactory.parser(
                        ConfigProviderFactory.EConfigType.JSON);
        assertNotNull(parser);

        Properties properties = new Properties();
        properties.load(new FileInputStream(BASE_PROPS_FILE));

        String filename = properties.getProperty(
                ConfigTestConstants.PROP_CONFIG_FILE);
        assertFalse(Strings.isNullOrEmpty(filename));
        String vs = properties.getProperty(ConfigTestConstants.PROP_CONFIG_VERSION);
        assertFalse(Strings.isNullOrEmpty(vs));
        Version version = Version.parse(vs);
        assertNotNull(version);

        try (ConfigFileReader reader = new ConfigFileReader(filename)) {
            ConfigurationSettings settings = new ConfigurationSettings();
            settings.setDownloadRemoteFiles(
                    ConfigurationSettings.EStartupOptions.OnStartUp);
            parser.parse("test-config", reader, settings, version, null);
            configuration = parser.getConfiguration();
            assertNotNull(configuration);
        }
    }

    @Test
    void find() {
        try {
            String path = "configuration/node_1";
            AbstractConfigNode node = configuration.find(path);
            assertNotNull(node);
            assertEquals(path, node.getSearchPath());
            path = "TEST_ELEMENT_LIST";
            node = node.find(path);
            assertTrue(node instanceof ConfigListElementNode);
            assertEquals(4, ((ConfigListElementNode) node).size());
            assertEquals(path, node.getName());
            LogUtils.debug(getClass(), node.getAbsolutePath());
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }

    @Test
    void update() {
        try {
            String path =
                    "configuration/node_1/node_2/node_3/node_4/TEST_VALUE_LIST";
            AbstractConfigNode node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigListValueNode);
            assertEquals(path, node.getSearchPath());

            ConfigListValueNode nl = (ConfigListValueNode) node;
            assertEquals(8, nl.size());
            LogUtils.debug(getClass(), node.getAbsolutePath());
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }

    @Test
    void findProperties() {
        try {
            String path = "configuration$";
            AbstractConfigNode node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigPropertiesNode);
            assertEquals(path, node.getSearchPath());

            path = "$PROP_1";
            node = configuration.find(node, path);
            assertTrue(node instanceof ConfigValueNode);
            String param = ((ConfigValueNode) node).getValue();
            assertFalse(Strings.isNullOrEmpty(param));
            debug(getClass(),
                  String.format("[path=%s] property value = %s", path, param));
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }

    @Test
    void findParameters() {
        try {
            String path = "configuration/node_1#";
            AbstractConfigNode node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigParametersNode);
            assertEquals(path, node.getSearchPath());

            path = "#PARAM_1";
            node = configuration.find(node, path);
            assertTrue(node instanceof ConfigValueNode);
            String param = ((ConfigValueNode) node).getValue();
            assertFalse(Strings.isNullOrEmpty(param));
            debug(getClass(),
                  String.format("[path=%s] parameter value = %s", path, param));

            path = "configuration/node_1/node_2#";
            node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigParametersNode);
            debug(getClass(), node);
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }

    @Test
    void findAttribute() {
        try {
            String path = "configuration/node_1/node_2@";
            AbstractConfigNode node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigAttributesNode);
            assertEquals(path, node.getSearchPath());

            path = "@ATTRIBUTE_4";
            node = configuration.find(node, path);
            assertTrue(node instanceof ConfigValueNode);
            String param = ((ConfigValueNode) node).getValue();
            assertFalse(Strings.isNullOrEmpty(param));
            debug(getClass(),
                  String.format("[path=%s] parameter value = %s", path, param));
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }

    @Test
    void findForConfigNode() {
        try {
            String path = "configuration/node_1/TEST_ELEMENT_LIST";
            AbstractConfigNode node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigListElementNode);
            assertEquals(path, node.getSearchPath());
            assertEquals(4, ((ConfigListElementNode) node).size());

            path = "TEST_ELEMENT_LIST[2]/string_2";
            node = configuration.find(node, path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigValueNode);
            String value = ((ConfigValueNode) node).getValue();
            assertFalse(Strings.isNullOrEmpty(value));
            debug(getClass(), String.format("[path=%s][value=%s]", path, value));
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }

    @Test
    void searchWildcard() {
        try {
            String path = "configuration/node_1/node_2/node_3/*";
            AbstractConfigNode node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigSearchListNode);
            debug(getClass(), node);

            path = "*/createdBy";
            node = configuration.find(node, path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigPathNode);
            debug(getClass(), node);

            path = "configuration/node_1/node_2/node_3/*/TEST_VALUE_LIST";
            node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigListValueNode);
            debug(getClass(), node);
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }

    @Test
    void searchRecursiveWildcard() {
        try {
            String path = "**/node_3/*";
            AbstractConfigNode node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigSearchListNode);
            debug(getClass(), node);

            path = "*/createdBy";
            node = configuration.find(node, path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigPathNode);
            debug(getClass(), node);

            path = "/**/node_2/node_3/*/TEST_VALUE_LIST";
            node = node.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigListValueNode);
            debug(getClass(), node);
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }

    @Test
    void searchParent() {
        try {
            String path = "**/node_3";
            AbstractConfigNode node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigPathNode);
            debug(getClass(), node);

            path = "../createdBy/../node_3/node_4/../updatedBy";
            node = node.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigPathNode);
            debug(getClass(), node);
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }
}