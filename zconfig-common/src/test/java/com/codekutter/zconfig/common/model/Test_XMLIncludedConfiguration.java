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
 * Date: 27/2/19 11:40 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.model;

import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigProviderFactory;
import com.codekutter.zconfig.common.ConfigTestConstants;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPropertiesNode;
import com.codekutter.zconfig.common.parsers.JSONConfigParser;
import com.codekutter.zconfig.common.parsers.XMLConfigParser;
import com.codekutter.zconfig.common.readers.ConfigFileReader;
import com.google.common.base.Strings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.util.Properties;

import static com.codekutter.common.utils.LogUtils.error;
import static org.junit.jupiter.api.Assertions.*;

class Test_XMLIncludedConfiguration {
    private static final String BASE_PROPS_FILE =
            "src/test/resources/XML/test-config-include.properties";
    private static Configuration configuration = null;

    @BeforeAll
    static void init() throws Exception {
        XMLConfigParser parser =
                (XMLConfigParser) ConfigProviderFactory.parser(
                        ConfigProviderFactory.EConfigType.XML);
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
            parser.parse("test-config-include", reader, settings, version, null);
            configuration = parser.getConfiguration();
            assertNotNull(configuration);
        }
    }

    @Test
    void find() {
        try {
            String path =
                    "configuration/node_1/root-node/node_1/node_2/node_3/node_4$";
            AbstractConfigNode node = configuration.find(path);
            assertNotNull(node);
            assertTrue(node instanceof ConfigPropertiesNode);
            assertEquals(3, ((ConfigPropertiesNode) node).getKeyValues().size());
            LogUtils.debug(getClass(), node.getAbsolutePath());
            LogUtils.debug(getClass(), node);
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }
}