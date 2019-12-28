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
 * Date: 3/1/19 9:21 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.parsers;

import com.codekutter.zconfig.common.ConfigProviderFactory;
import com.codekutter.zconfig.common.ConfigTestConstants;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.readers.ConfigFileReader;
import com.codekutter.zconfig.common.readers.ConfigURLReader;
import com.google.common.base.Strings;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.util.Properties;

import static com.codekutter.common.utils.LogUtils.*;
import static org.junit.jupiter.api.Assertions.*;

class Test_JSONConfigParser {
    private static final String BASIC_PROPS_FILE =
            "src/test/resources/json/test-config.properties";
    private static final String INCLUDED_PROPS_FILE =
            "src/test/resources/json/test-config-include.properties";

    @Test
    void parseFromFile() {
        try {
            JSONConfigParser parser =
                    (JSONConfigParser) ConfigProviderFactory.parser(
                            ConfigProviderFactory.EConfigType.JSON);
            assertNotNull(parser);

            Properties properties = new Properties();
            properties.load(new FileInputStream(BASIC_PROPS_FILE));

            String filename =
                    properties.getProperty(ConfigTestConstants.PROP_CONFIG_FILE);
            assertFalse(Strings.isNullOrEmpty(filename));
            String vs =
                    properties.getProperty(ConfigTestConstants.PROP_CONFIG_VERSION);
            assertFalse(Strings.isNullOrEmpty(vs));
            Version version = Version.parse(vs);
            assertNotNull(version);

            try (ConfigFileReader reader = new ConfigFileReader(filename)) {

                parser.parse("test-config", reader, null, version, null);
                Configuration configuration = parser.getConfiguration();
                assertNotNull(configuration);

                debug(getClass(), configuration);
            }
        } catch (Throwable t) {
            error(getClass(), t);
            fail(t);
        }
    }

    @Test
    void parseFromURL() {
        try {
            JSONConfigParser parser =
                    (JSONConfigParser) ConfigProviderFactory.parser(
                            ConfigProviderFactory.EConfigType.JSON);
            assertNotNull(parser);

            Properties properties = new Properties();
            properties.load(new FileInputStream(BASIC_PROPS_FILE));

            String url =
                    properties.getProperty(ConfigTestConstants.PROP_CONFIG_URL);
            assertFalse(Strings.isNullOrEmpty(url));
            String vs =
                    properties.getProperty(ConfigTestConstants.PROP_CONFIG_VERSION);
            assertFalse(Strings.isNullOrEmpty(vs));
            Version version = Version.parse(vs);
            assertNotNull(version);

            try (ConfigURLReader reader = new ConfigURLReader(url)) {

                parser.parse("test-config", reader, null, version, null);
                Configuration configuration = parser.getConfiguration();
                assertNotNull(configuration);

                debug(getClass(), configuration);
            }
        } catch (Throwable t) {
            error(getClass(), t);
            fail(t);
        }
    }

    @Test
    void parseFromFileWithInclude() {
        try {
            JSONConfigParser parser =
                    (JSONConfigParser) ConfigProviderFactory.parser(
                            ConfigProviderFactory.EConfigType.JSON);
            assertNotNull(parser);

            Properties properties = new Properties();
            properties.load(new FileInputStream(INCLUDED_PROPS_FILE));

            String filename =
                    properties.getProperty(ConfigTestConstants.PROP_CONFIG_FILE);
            assertFalse(Strings.isNullOrEmpty(filename));
            String vs =
                    properties.getProperty(ConfigTestConstants.PROP_CONFIG_VERSION);
            assertFalse(Strings.isNullOrEmpty(vs));
            Version version = Version.parse(vs);
            assertNotNull(version);

            try (ConfigFileReader reader = new ConfigFileReader(filename)) {

                parser.parse("test-config-include", reader, null, version, null);
                Configuration configuration = parser.getConfiguration();
                assertNotNull(configuration);

                debug(getClass(), configuration);
            }
        } catch (Throwable t) {
            error(getClass(), t);
            fail(t);
        }
    }
}