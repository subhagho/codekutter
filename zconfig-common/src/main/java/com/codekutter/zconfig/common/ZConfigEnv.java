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
 * Date: 13/2/19 10:19 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.StateException;
import com.codekutter.common.utils.KeyStoreVault;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.NetUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.parsers.AbstractConfigParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract base class for defining the operating environment.
 */
public abstract class ZConfigEnv {
    /**
     * Name of this configuration instance.
     */
    private String configName;
    /**
     * Parsed configuration handle.
     */
    private Configuration configuration;

    /**
     * Client instance state.
     */
    private EnvState state = new EnvState();

    protected ZConfigEnv(@Nonnull String configName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configName));
        this.configName = configName;
    }

    /**
     * Initialize this client environment from the specified configuration file and version.
     *
     * @param configfile - Configuration file path.
     * @param version    - Configuration version (expected)
     * @param password   - Password (required if configuration has encryption)
     * @throws ConfigurationException
     */
    protected final void init(@Nonnull String configfile, @Nonnull Version version, String password)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configfile));
        Preconditions.checkArgument(version != null);
        try {
            AbstractConfigParser parser = ConfigProviderFactory.parser(configfile);
            if (parser == null) {
                throw new ConfigurationException(String.format(
                        "Cannot get configuration parser instance. [file=%s]",
                        configfile));
            }
            init(parser, configfile, version, password);
        } catch (Exception e) {
            state.setError(e);
            throw new ConfigurationException(e);
        }
    }

    /**
     * Initialize this client environment from the specified configuration file and version.
     *
     * @param configfile - Configuration file path.
     * @param type       - Configuration file type (in-case file type cannot be deciphered).
     * @param version    - Configuration version (expected)
     * @param password   - Password (required if configuration has encryption)
     * @throws ConfigurationException
     */
    protected final void init(@Nonnull String configfile,
                              @Nonnull ConfigProviderFactory.EConfigType type,
                              @Nonnull Version version, String password)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configfile));
        Preconditions.checkArgument(version != null);
        Preconditions.checkArgument(type != null);

        try {
            AbstractConfigParser parser = ConfigProviderFactory.parser(type);
            if (parser == null) {
                throw new ConfigurationException(String.format(
                        "Cannot get configuration parser instance. [type=%s]",
                        type.name()));
            }
            init(parser, configfile, version, password);
        } catch (Exception e) {
            state.setError(e);
            throw new ConfigurationException(e);
        }
    }

    /**
     * Initialize this client environment from the specified configuration file and version
     * using the configuration parser.
     *
     * @param parser     - Configuration parser to use.
     * @param configfile - Configuration file path.
     * @param version    - Configuration version (expected)
     * @param password   - Password (required if configuration has encryption)
     * @throws ConfigurationException
     */
    protected final void init(@Nonnull AbstractConfigParser parser, @Nonnull String configfile,
                              @Nonnull Version version, String password)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configfile));
        Preconditions.checkArgument(version != null);
        Preconditions.checkArgument(parser != null);
        try {
            KeyStoreVault vault = new KeyStoreVault();
            // Using dummy config node.
            vault.configure(new ConfigPathNode());

            ConfigKeyVault.getInstance().withVault(vault);

            LogUtils.info(getClass(), String.format(
                    "Initializing Client Environment : With Configuration file [%s]...",
                    configfile));
            Path path = Paths.get(configfile);
            parser.parse(configName, ConfigProviderFactory.reader(path.toUri()),
                    null,
                    version, password);
            configuration = parser.getConfiguration();
            if (configuration == null) {
                throw new ConfigurationException(String.format(
                        "Error parsing configuration : NULL configuration read. [file=%s]",
                        configfile));
            }

            postInit();
            updateState(EEnvState.Initialized);
        } catch (Exception e) {
            LogUtils.error(getClass(), e);
            throw new ConfigurationException(e);
        }
    }

    /**
     * Setup the instance header data.
     *
     * @param type     - Instance Tpe.
     * @param instance - Instance handle.
     * @throws ConfigurationException
     */
    protected void setupInstance(@Nonnull Class<? extends ZConfigInstance> type,
                                 @Nonnull ZConfigInstance instance)
            throws ConfigurationException {
        Preconditions.checkArgument(type != null);
        Preconditions.checkArgument(instance != null);

        instance.setId(UUID.randomUUID().toString());
        instance.setStartTime(DateTime.now());
        ConfigurationAnnotationProcessor
                .readConfigAnnotations(type,
                        getConfiguration(),
                        instance);
        InetAddress addr = NetUtils.getIpAddress();
        if (addr != null) {
            instance.setIp(addr.getHostAddress());
            instance.setHostname(addr.getCanonicalHostName());
        }
        instance.setApplicationGroup(configuration.getApplicationGroup());
        instance.setApplicationName(configuration.getApplication());
    }

    /**
     * Update the state of this instance.
     *
     * @param state - State to update to.
     */
    protected void updateState(@Nonnull EEnvState state) {
        Preconditions.checkArgument(state != null);
        this.state.setState(state);
    }

    /**
     * Check the state of this instance.
     *
     * @param state - Expected state.
     * @throws StateException - Exception will be raised if state is not as expected.
     */
    protected void checkState(@Nonnull EEnvState state) throws StateException {
        Preconditions.checkArgument(state != null);
        this.state.checkState(state);
    }

    /**
     * Disposed this client environment instance.
     */
    protected void dispose() {
        if (state.getState() == EEnvState.Initialized) {
            state.dispose();
        }
    }

    /**
     * Get the state of this client environment.
     *
     * @return - Current state.
     */
    public EEnvState getState() {
        return state.getState();
    }

    /**
     * Get configuration loaded for this client environment.
     *
     * @return - Configuration handle.
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Get a new instance of the JSON Object mapper.
     *
     * @return - JSON Object mapper.
     */
    public ObjectMapper getJsonMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    /**
     * Perform post-initialisation tasks if any.
     *
     * @throws ConfigurationException
     */
    public abstract void postInit() throws ConfigurationException;

    /**
     * Client environment singleton.
     */
    private static ZConfigEnv __env = null;

    /**
     * Environment Instance Lock.
     */
    private static ReentrantLock _envLock = new ReentrantLock();


    /**
     * Shutdown this client environment.
     */
    public static void shutdown() {
        try {
            if (__env != null) {
                getEnvLock();
                try {
                    __env.dispose();
                    __env = null;
                } finally {
                    releaseEnvLock();
                }
            }
        } catch (Exception ex) {
            LogUtils.error(ZConfigEnv.class, ex);
        }
    }


    /**
     * Get a handle to the client environment singleton.
     *
     * @return - Environment handle.
     * @throws EnvException
     */
    public static ZConfigEnv env() throws EnvException {
        try {
            if (__env != null)
                __env.checkState(EEnvState.Initialized);
            return __env;
        } catch (StateException e) {
            throw new EnvException(e);
        }
    }

    /**
     * Initialize the ENV handle.
     *
     * @param type - Type of the env instance.
     * @return - Created Env handle.
     * @throws EnvException - Exception raised if initialization lock not acquired by current thread.
     */
    @SuppressWarnings("unchecked")
    protected static ZConfigEnv initialize(@Nonnull Class<? extends ZConfigEnv> type, @Nonnull String configName)
            throws EnvException {
        if (!_envLock.isLocked() || !_envLock.isHeldByCurrentThread()) {
            throw new EnvException("Environment not locked for initialisation.");
        }
        try {
            if (__env == null) {
                Constructor<? extends ZConfigEnv> ctor = (Constructor<? extends ZConfigEnv>) ReflectionUtils.getConstructor(type, String.class);
                __env = ctor.newInstance(configName);
                LogUtils.info(ZConfigEnv.class,
                        String.format("Created ENV instance with type [%s]...",
                                type.getCanonicalName()));
            }
            return __env;
        } catch (Exception ex) {
            throw new EnvException(ex);
        }
    }

    /**
     * Get the env initialization lock.
     *
     * @throws EnvException - Exception raised if Env has already been disposed.
     */
    protected static void getEnvLock() throws EnvException {
        if (__env != null && __env.state.getState() == EEnvState.Disposed) {
            throw new EnvException("Environment has already been disposed.");
        }
        _envLock.lock();
    }

    /**
     * Release the env initialization lock.
     *
     * @throws EnvException - Exception raised if current thread doesn't hold the lock.
     */
    protected static void releaseEnvLock() throws EnvException {
        if (__env != null && __env.state.getState() == EEnvState.Disposed) {
            throw new EnvException("Environment has already been disposed.");
        }
        if (_envLock.isLocked() && _envLock.isHeldByCurrentThread()) {
            _envLock.unlock();
        } else {
            throw new EnvException(String.format(
                    "Lock not acquired or held by another thread. [thread id=%d]",
                    Thread.currentThread().getId()));
        }
    }
}
