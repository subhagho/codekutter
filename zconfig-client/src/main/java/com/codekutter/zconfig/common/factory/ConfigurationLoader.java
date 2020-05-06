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
 * Date: 26/2/19 8:55 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.factory;

import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigProviderFactory;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.ConfigurationSettings;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.parsers.AbstractConfigParser;
import com.codekutter.zconfig.common.readers.AbstractConfigReader;
import com.codekutter.zconfig.common.readers.ConfigFileReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Class to load configurations.
 */
public class ConfigurationLoader {
    /**
     * Load configuration from the specified URI.
     *
     * @param configName - Configuration name.
     * @param configUri  - Configuration URI string.
     * @param configType - Configuration Parser type.
     * @param version    - Configuration URI String.
     * @param settings   - Configuration Settings.
     * @return - Loaded Configuration instance.
     * @throws ConfigurationException
     */
    public Configuration load(@Nonnull String configName, @Nonnull String configUri,
                              @Nonnull ConfigProviderFactory.EConfigType configType,
                              @Nonnull Version version,
                              ConfigurationSettings settings, String password)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configUri));
        Preconditions.checkArgument(version != null);
        Preconditions.checkArgument(configType != null);

        try {
            URI uri = new URI(configUri);
            LogUtils.debug(getClass(),
                    String.format(
                            "Reading configuration: [type=%s][uri=%s][version=%s]",
                            configType.name(), uri.toString(),
                            version.toString()));
            try (AbstractConfigReader reader = ConfigProviderFactory.reader(uri)) {
                if (reader == null) {
                    throw new ConfigurationException(
                            String.format("Error getting reader for URI: [uri=%s]",
                                    configUri));
                }
                try (
                        AbstractConfigParser parser =
                                ConfigProviderFactory.parser(configType)) {
                    if (parser == null) {
                        throw new ConfigurationException(
                                String.format(
                                        "Error getting parser for type: [type=%s]",
                                        configType.name()));
                    }
                    parser.parse(configName, reader, settings, version, password);
                    return parser.getConfiguration();
                }
            }
        } catch (URISyntaxException e) {
            throw new ConfigurationException(e);
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Load configuration from the specified URI.
     *
     * @param configName - Configuration name.
     * @param configUri  - Configuration URI string.
     * @param configType - Configuration Parser type.
     * @param version    - Configuration URI String.
     * @return - Loaded Configuration instance.
     * @throws ConfigurationException
     */
    public Configuration load(@Nonnull String configName, @Nonnull String configUri,
                              @Nonnull ConfigProviderFactory.EConfigType configType,
                              @Nonnull Version version, String password)
            throws ConfigurationException {
        return load(configName, configUri, configType, version, null, password);
    }

    /**
     * Load configuration from the specified filename.
     *
     * @param configName - Configuration name.
     * @param filename   - Configuration filename.
     * @param version    - Configuration URI String.
     * @param settings   - Configuration Settings.
     * @return - Loaded Configuration instance.
     * @throws ConfigurationException
     */
    public Configuration load(@Nonnull String configName, @Nonnull String filename,
                              @Nonnull Version version,
                              ConfigurationSettings settings, String password)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(filename));
        Preconditions.checkArgument(version != null);

        LogUtils.debug(getClass(),
                String.format(
                        "Reading configuration: [name=%s][filename=%s][version=%s]",
                        configName, filename,
                        version.toString()));
        try {
            try (ConfigFileReader reader = new ConfigFileReader(filename)) {
                try (
                        AbstractConfigParser parser = ConfigProviderFactory
                                .parser(filename)) {
                    if (parser == null) {
                        throw new ConfigurationException(
                                String.format(
                                        "Error getting parser for file: [filename=%s]",
                                        filename));
                    }
                    parser.parse(configName, reader, settings, version, password);
                    return parser.getConfiguration();
                }
            }
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Load configuration from the specified filename.
     *
     * @param configName - Configuration name.
     * @param filename   - Configuration filename.
     * @param version    - Configuration URI String.
     * @return - Loaded Configuration instance.
     * @throws ConfigurationException
     */
    public Configuration load(@Nonnull String configName, @Nonnull String filename,
                              @Nonnull Version version, String password)
            throws ConfigurationException {
        return load(configName, filename, version, null, password);
    }
}
