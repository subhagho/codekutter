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
 * Date: 9/2/19 10:09 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.utils.DefaultUniqueIDGenerator;
import com.codekutter.common.utils.IUniqueIDGenerator;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.core.zookeeper.ZkConnectionConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

/**
 * Singleton instance for setting up the core operating environment.
 */
public class ZConfigCoreEnv extends ZConfigEnv {
    /**
     * Configuration name for ZConfig Core configurations.
     */
    public static final String CONFIG_NAME = "zconfig-core";

    private ZConfigCoreInstance instance;
    private ZkConnectionConfig zkConnectionConfig;
    private IUniqueIDGenerator idGenerator = new DefaultUniqueIDGenerator();

    /**
     * Default constructor - Sets the name of the config.
     */
    public ZConfigCoreEnv() {
        super(CONFIG_NAME);
    }

    /**
     * Perform post-initialisation tasks if any.
     *
     * @throws ConfigurationException
     */
    @Override
    public void postInit() throws ConfigurationException {
        instance = new ZConfigCoreInstance();
        setupInstance(ZConfigCoreInstance.class, instance);
        LogUtils.debug(getClass(), instance);

        zkConnectionConfig = new ZkConnectionConfig();
        ConfigurationAnnotationProcessor
                .readConfigAnnotations(ZkConnectionConfig.class, getConfiguration(),
                        zkConnectionConfig);
        LogUtils.debug(getClass(), zkConnectionConfig);
        LogUtils.info(getClass(),
                "Core environment successfully initialized...");
    }

    /**
     * Client environment singleton.
     */
    public ZConfigCoreInstance getInstance() {
        return instance;
    }

    /**
     * Get the ZooKeeper connection configuration.
     *
     * @return - ZooKeeper connection configuration.
     */
    public ZkConnectionConfig getZkConnectionConfig() {
        return zkConnectionConfig;
    }

    /**
     * Get the Unique ID Generator handle.
     *
     * @return - Unique ID Generator
     */
    public IUniqueIDGenerator getIdGenerator() {
        return idGenerator;
    }

    /**
     * Setup the client environment using the passed configuration file.
     *
     * @param configfile - Configuration file (path) to read from.
     * @param version    - Configuration version (expected)
     * @throws ConfigurationException
     */
    public static void setup(@Nonnull String configName,
                             @Nonnull String configfile,
                             @Nonnull String version,
                             String password)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configfile));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(version));

        try {
            ZConfigEnv.getEnvLock();
            try {
                ZConfigEnv env = ZConfigEnv.initialize(ZConfigCoreEnv.class, configName);
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
                ZConfigEnv env = ZConfigEnv.initialize(ZConfigCoreEnv.class, configName);
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
     * Get a handle to the core environment singleton.
     *
     * @return - Core Environment handle.
     * @throws EnvException
     */
    public static ZConfigCoreEnv coreEnv() throws EnvException {
        ZConfigEnv env = ZConfigEnv.env();
        if (env instanceof ZConfigCoreEnv) {
            return (ZConfigCoreEnv) env;
        }
        throw new EnvException(
                String.format("Env handle is not of client type. [type=%s]",
                        env.getClass().getCanonicalName()));
    }
}
