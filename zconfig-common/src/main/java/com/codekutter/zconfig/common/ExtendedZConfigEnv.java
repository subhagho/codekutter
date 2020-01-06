/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.locking.DistributedLockFactory;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.Monitoring;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

public class ExtendedZConfigEnv extends ZConfigEnv {
    public static final String CONFIG_ENV_PATH = "/configuration/env";

    protected ConfigPathNode envNode = null;

    protected ExtendedZConfigEnv(@Nonnull String configName) {
        super(configName);
    }

    /**
     * Perform post-initialisation tasks if any.
     *
     * @throws ConfigurationException
     */
    @Override
    public void postInit() throws ConfigurationException {
        Preconditions.checkState(getConfiguration() != null);
        Preconditions.checkState(getConfiguration().getRootConfigNode() != null);

        AbstractConfigNode node = getConfiguration().getRootConfigNode().find(CONFIG_ENV_PATH);
        if (node instanceof ConfigPathNode) {
            envNode = (ConfigPathNode) node;

            setupMonitoring();
            setupLockFactory();
        } else {
            envNode = null;
        }
    }

    private void setupLockFactory() throws ConfigurationException {
        AbstractConfigNode node = ConfigUtils.getPathNode(DistributedLockFactory.class, envNode);
        if ((node instanceof ConfigPathNode) || (node instanceof ConfigListElementNode)) {
            DistributedLockFactory.setup(node);
        }
    }

    private void setupMonitoring() throws ConfigurationException {
        Monitoring.MonitorConfig config = ConfigurationAnnotationProcessor.readConfigAnnotations(Monitoring.MonitorConfig.class, envNode);
        if (config == null) {
            config = new Monitoring.MonitorConfig();
        }
        int reporters = 0;
        if (config.enableJmx()) {
            reporters = reporters | Monitoring.REPORTER_JMX;
        }
        if (config.enableSlf4j()) {
            reporters = reporters | Monitoring.REPORTER_SLF4J;
        }
        if (config.enableFileLogging()) {
            reporters = reporters | Monitoring.REPORTER_CSV;
        }
        Monitoring.start(config.namespace(), reporters, config.fileLoggerDir(), config.enableMemoryStats(), config.enableGcStats());
        LogUtils.info(getClass(), "Initialized monitoring...");
        LogUtils.debug(getClass(), config);
    }

    /**
     * Disposed this client environment instance.
     */
    @Override
    protected void dispose() {
        super.dispose();
        Monitoring.stop();
        DistributedLockFactory.close();
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
                ZConfigEnv env = ZConfigEnv.initialize(ExtendedZConfigEnv.class, configName);
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
    public static ExtendedZConfigEnv env() throws EnvException {
        ZConfigEnv env = ZConfigEnv.env();
        if (env instanceof ExtendedZConfigEnv) {
            return (ExtendedZConfigEnv) env;
        }
        throw new EnvException(
                String.format("Env handle is not of client type. [type=%s]",
                        env.getClass().getCanonicalName()));
    }
}
