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
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;

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
            envNode = (ConfigPathNode)node;

            setupMonitoring();
            setupLockFactory();
        } else {
            envNode = null;
        }
    }

    private void setupLockFactory() throws ConfigurationException {
        AbstractConfigNode node = ConfigUtils.getPathNode(DistributedLockFactory.class, envNode);
        if (node instanceof ConfigPathNode) {
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
}
