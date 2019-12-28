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
 * Date: 5/1/19 11:01 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.writers;

import com.codekutter.zconfig.common.ConfigProviderFactory;
import com.codekutter.zconfig.common.ConfigTestConstants;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.parsers.JSONConfigParser;
import com.codekutter.zconfig.common.readers.ConfigFileReader;
import com.google.common.base.Strings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import static com.codekutter.common.utils.LogUtils.*;
import static org.junit.jupiter.api.Assertions.*;

class Test_JSONFileConfigWriter {
    private static final String JSON_FILE =
            "src/test/resources/json/test-config.properties";
    private static final String TEMP_OUTDIR = "/tmp/zconfig/test/output";

    private static Configuration configuration = null;

    @BeforeAll
    static void init() throws Exception {
        File dir = new File(TEMP_OUTDIR);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new Exception(String.format(
                        "Error creating test output folder. [directory=%s]",
                        dir.getAbsolutePath()));
            }
        }

        JSONConfigParser parser =
                (JSONConfigParser) ConfigProviderFactory.parser(
                        ConfigProviderFactory.EConfigType.JSON);
        assertNotNull(parser);

        Properties properties = new Properties();
        properties.load(new FileInputStream(JSON_FILE));

        String filename =
                properties.getProperty(ConfigTestConstants.PROP_CONFIG_FILE);
        assertFalse(Strings.isNullOrEmpty(filename));
        String vs = properties.getProperty(ConfigTestConstants.PROP_CONFIG_VERSION);
        assertFalse(Strings.isNullOrEmpty(vs));
        Version version = Version.parse(vs);
        assertNotNull(version);

        try (ConfigFileReader reader = new ConfigFileReader(filename)) {

            parser.parse("test-config", reader, null, version, null);
            configuration = parser.getConfiguration();
            assertNotNull(configuration);
        }
    }

    @Test
    void write() {
        try {
            assertNotNull(configuration);

            JSONFileConfigWriter writer =
                    (JSONFileConfigWriter) ConfigProviderFactory.writer(
                            ConfigProviderFactory.EConfigType.JSON);
            assertNotNull(writer);

            String outfile = writer.write(configuration, TEMP_OUTDIR);
            assertFalse(Strings.isNullOrEmpty(outfile));

            File outf = new File(outfile);
            assertTrue(outf.exists());
            debug(getClass(), String.format("Created configuration : file=%s",
                                            outf.getAbsolutePath()));
        } catch (Throwable e) {
            error(getClass(), e);
            fail(e);
        }
    }
}