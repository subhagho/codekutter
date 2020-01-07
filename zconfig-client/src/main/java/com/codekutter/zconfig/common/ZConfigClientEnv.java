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
 * Date: 10/2/19 5:48 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.GlobalConstants;
import com.codekutter.common.model.EReaderType;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.factory.ConfigurationManager;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.ConfigurationSettings;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Singleton class to define and expose environment settings.
 */
public class ZConfigClientEnv extends ZConfigEnv {
    /**
     * Root configuration node for the client.
     */
    public static final String CONFIG_NODE_ZCONFIG = "zconfig";
    /**
     * Configuration server configuration node.
     */
    public static final String CONFIG_NODE_SERVER = "server";
    /**
     * Type of the configuration server URI. (file/http(s))
     */
    public static final String CONFIG_NODE_SERVER_TYPE = "server/type";
    /**
     * Host IP/name of the configuration server (required if type is HTTP(s))
     */
    public static final String CONFIG_NODE_SERVER_HOST = "server/host";
    /**
     * Host port of the configuration server (optional if type is HTTP(s))
     */
    public static final String CONFIG_NODE_SERVER_PORT = "server/port";
    /**
     * Base path of the configuration server
     */
    public static final String CONFIG_NODE_SERVER_BASE_PATH = "server/basePath";

    /**
     * Configuration name for ZConfig Client configurations.
     */
    public static final String CONFIG_NAME = "zconfig-client";

    /**
     * Client instance handle.
     */
    private ZConfigClientInstance instance;

    /**
     * Configuration manager instance.
     */
    private ConfigurationManager configurationManager = new ConfigurationManager();

    /**
     * Update listener instance.
     */
    private ConfigurationUpdateHandler updateHandler =
            new ConfigurationUpdateHandler();

    /**
     * URI of the host configuration server or local folder where configuration(s) are
     * loaded from.
     */
    private URI serverUri;

    /**
     * Configuration settings to use for parsing.
     */
    private ConfigurationSettings settings;

    /**
     * Default constructor - Sets the name of the config.
     */
    protected ZConfigClientEnv() {
        super(CONFIG_NAME);
    }

    /**
     * Perform post-initialisation tasks if any.
     *
     * @throws ConfigurationException
     */
    @Override
    public void postInit() throws ConfigurationException {
        instance = new ZConfigClientInstance();
        setupInstance(ZConfigClientInstance.class, instance);
        LogUtils.debug(getClass(), instance);

        parseServerUri();

        AbstractConfigNode node = getConfiguration().find(CONFIG_NODE_ZCONFIG);
        if (!(node instanceof ConfigPathNode)) {
            throw new ConfigurationException(
                    String.format("Base configuration node not found. [node=%s]",
                            CONFIG_NODE_ZCONFIG));
        }
        settings = new ConfigurationSettings();
        ConfigurationAnnotationProcessor
                .readConfigAnnotations(ConfigurationSettings.class,
                        (ConfigPathNode) node, settings);
        LogUtils.debug(getClass(), settings);
        LogUtils.info(getClass(),
                "Client environment successfully initialized...");
        configurationManager.add(getConfiguration());
    }

    /**
     * Parse he configuration server information.
     *
     * @throws ConfigurationException
     */
    private void parseServerUri() throws ConfigurationException {
        try {
            String path =
                    String.format("%s/%s", CONFIG_NODE_ZCONFIG, CONFIG_NODE_SERVER);
            AbstractConfigNode node = getConfiguration().find(path);
            if ((node instanceof ConfigPathNode)) {
                ConfigPathNode pathNode = (ConfigPathNode) node;
                AbstractConfigNode configNode =
                        pathNode.find(CONFIG_NODE_SERVER_TYPE);
                if (!(configNode instanceof ConfigValueNode)) {
                    throw ConfigurationException
                            .propertyNotFoundException(CONFIG_NODE_SERVER_TYPE);
                }
                ConfigValueNode vn = (ConfigValueNode) configNode;
                String type = vn.getValue();
                if (Strings.isNullOrEmpty(type)) {
                    throw ConfigurationException
                            .propertyNotFoundException(CONFIG_NODE_SERVER_TYPE);
                }
                EReaderType readerType = EReaderType.parse(type);
                if (readerType == null) {
                    throw new ConfigurationException(
                            String.format("Invalid Server Type : [type=%s]", type));
                }

                configNode = pathNode.find(CONFIG_NODE_SERVER_BASE_PATH);
                if (!(configNode instanceof ConfigValueNode)) {
                    throw ConfigurationException
                            .propertyNotFoundException(
                                    CONFIG_NODE_SERVER_BASE_PATH);
                }
                vn = (ConfigValueNode) configNode;
                String basePath = vn.getValue();
                if (Strings.isNullOrEmpty(basePath)) {
                    throw ConfigurationException
                            .propertyNotFoundException(
                                    CONFIG_NODE_SERVER_BASE_PATH);
                }
                if (readerType == EReaderType.File) {
                    String uri = String.format("%s://%s",
                            EReaderType.getURIScheme(readerType),
                            basePath);
                    serverUri = new URI(uri);
                } else if (readerType == EReaderType.HTTP ||
                        readerType == EReaderType.HTTPS) {
                    configNode = pathNode.find(CONFIG_NODE_SERVER_HOST);
                    if (!(configNode instanceof ConfigValueNode)) {
                        throw ConfigurationException
                                .propertyNotFoundException(
                                        CONFIG_NODE_SERVER_HOST);
                    }
                    vn = (ConfigValueNode) configNode;
                    String host = vn.getValue();
                    if (Strings.isNullOrEmpty(host)) {
                        throw ConfigurationException
                                .propertyNotFoundException(
                                        CONFIG_NODE_SERVER_HOST);
                    }
                    configNode = pathNode.find(CONFIG_NODE_SERVER_PORT);
                    if (!(configNode instanceof ConfigValueNode)) {
                        throw ConfigurationException
                                .propertyNotFoundException(
                                        CONFIG_NODE_SERVER_PORT);
                    }
                    vn = (ConfigValueNode) configNode;
                    String port = vn.getValue();
                    if (!Strings.isNullOrEmpty(port)) {
                        String uri = String.format("%s://%s:%s/%s", EReaderType
                                .getURIScheme(readerType), host, port, basePath);
                        serverUri = new URI(uri);
                    } else {
                        String uri = String.format("%s://%s/%s", EReaderType
                                .getURIScheme(readerType), host, basePath);
                        serverUri = new URI(uri);
                    }
                }
            } else {
                throw new ConfigurationException(String.format(
                        "Configuration server settings not found: [path=%s]",
                        path));
            }
        } catch (URISyntaxException e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Get the request URI for the specified configuration/version.
     *
     * @param configName - Configuration name.
     * @param version    - Configuration Version.
     * @param configType - Type of configuration file.
     * @return - request URI path.
     */
    public String getRemoteConfigurationPath(@Nonnull String configName,
                                             @Nonnull Version version,
                                             @Nonnull
                                                     ConfigProviderFactory.EConfigType configType) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configName));
        Preconditions.checkArgument(version != null);
        String path = getRelativeConfigurationPath(configName, version, configType);
        if (serverUri.getScheme().equals(GlobalConstants.URI_SCHEME_FILE)) {
            path = String.format("%s/%s.%s", path, configName,
                    configType.name().toLowerCase());
        } else {
            path = String.format("%s/%s", path,
                    configType.name().toLowerCase());
        }
        return String.format("%s%s", serverUri.toString(), path);
    }

    /**
     * Get the relative URI path for the specified configuration/version.
     *
     * @param configName - Configuration name.
     * @param version    - Configuration Version.
     * @param configType - Type of configuration file.
     * @return - request URI path.
     */
    public String getRelativeConfigurationPath(@Nonnull String configName,
                                               @Nonnull Version version,
                                               @Nonnull
                                                       ConfigProviderFactory.EConfigType configType) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configName));
        Preconditions.checkArgument(version != null);
        return String.format("/%s/%s/%s/%d", instance.getApplicationGroup(),
                instance.getApplicationName(), configName,
                version.getMajorVersion());
    }

    /**
     * Get a configuration instance handle. If the configuration isn't yet loaded
     * it will be attempted to load.
     *
     * @param configName - Configuration Name.
     * @param version    - Requested Configuration version.
     * @param configType - Configuration type.
     * @return - Configuration instance.
     * @throws ConfigurationException
     */
    public Configuration getConfiguration(@Nonnull String configName,
                                          @Nonnull Version version,
                                          @Nonnull
                                                  ConfigProviderFactory.EConfigType configType,
                                          String password)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configName));
        Preconditions.checkArgument(version != null);
        Preconditions.checkArgument(configType != null);

        String configPath =
                getRemoteConfigurationPath(configName, version, configType);
        return configurationManager
                .load(configName, configPath, configType, version, password);
    }

    /**
     * Get an auto-wired instance of the specified type.
     *
     * @param type         - Auto-wired instance type.
     * @param configName   - Configuration name to auto-wire from.
     * @param relativePath - Relative search path.
     * @param <T>          - Type to instantiate.
     * @return - Auto-wired instance.
     * @throws ConfigurationException
     */
    public <T> T autowireType(@Nonnull Class<T> type, @Nonnull String configName,
                              String relativePath)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configName));
        Preconditions.checkArgument(type != null);

        return configurationManager.autowireType(type, configName, relativePath);
    }

    /**
     * Get the handle to the configuration manager.
     *
     * @return - Configuration Manager instance.
     */
    public ConfigurationManager getConfigurationManager() {
        return configurationManager;
    }

    /**
     * Get the handle to the configuration update listener.
     *
     * @return - Configuration Update Listener.
     */
    public ConfigurationUpdateHandler getUpdateHandler() {
        return updateHandler;
    }

    /**
     * Get the instance header for this client.
     *
     * @return - Client instance header.
     */
    public ZConfigClientInstance getInstance() {
        return instance;
    }

    /**
     * Setup the client environment using the passed configuration file.
     *
     * @param configfile - Configuration file (path) to read from.
     * @param version    - Configuration version (expected)
     * @throws ConfigurationException
     */
    public static void setup(@Nonnull String configName, @Nonnull String configfile, @Nonnull String version,
                             String password)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configfile));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(version));

        try {
            ZConfigEnv.getEnvLock();
            try {
                ZConfigEnv env = ZConfigEnv.initialize(ZConfigClientEnv.class, configName);
                if (env.getState() != EEnvState.Initialized) {
                    env.init(configfile, Version.parse(version), password);
                }
            } finally {
                ZConfigEnv.releaseEnvLock();
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Setup the client environment using the passed configuration file.
     * Method to be used in-case the configuration type cannot be deciphered using
     * the file extension.
     *
     * @param configfile - Configuration file (path) to read from.
     * @param type       - Configuration type.
     * @param version    - Configuration version (expected)
     * @throws ConfigurationException
     */
    public static void setup(@Nonnull String configName,
                             @Nonnull String configfile,
                             @Nonnull ConfigProviderFactory.EConfigType type,
                             @Nonnull String version, String password)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configfile));
        Preconditions.checkArgument(type != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(version));


        try {
            ZConfigEnv.getEnvLock();
            try {
                ZConfigEnv env = ZConfigEnv.initialize(ZConfigClientEnv.class, configName);
                if (env.getState() != EEnvState.Initialized) {
                    env.init(configfile, type, Version.parse(version), password);
                }
            } finally {
                ZConfigEnv.releaseEnvLock();
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Get the instance of the client environment handle.
     *
     * @return - Client environment handle.
     * @throws EnvException
     */
    public static ZConfigClientEnv clientEnv() throws EnvException {
        ZConfigEnv env = ZConfigEnv.env();
        if (env instanceof ZConfigClientEnv) {
            return (ZConfigClientEnv) env;
        }
        throw new EnvException(
                String.format("Env handle is not of client type. [type=%s]",
                        env.getClass().getCanonicalName()));
    }
}
