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
 * Date: 4/3/19 8:48 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.events.ConfigUpdateBatch;
import com.codekutter.zconfig.common.events.ConfigUpdateEvent;
import com.codekutter.zconfig.common.factory.ConfigurationManager;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.model.nodes.*;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Class used to apply update events to configurations.
 */
public class ConfigurationUpdateHandler {
    /**
     * Process and apply the batch of configuration update event.
     *
     * @param batch - Update Batch.
     * @throws ConfigurationException
     */
    public void processEvents(@Nonnull ConfigUpdateBatch batch) throws
            ConfigurationException {
        Preconditions.checkArgument(batch != null);
        Preconditions.checkArgument(!batch.getEvents().isEmpty());

        try {
            LogUtils.info(getClass(), String.format(
                    "Applying update to configuration [name=%s][transaction=%s]",
                    batch.getHeader().getConfigName(),
                    batch.getHeader().getTransactionId()));
            batch.validate();
            String configName = null;
            List<String> updatePaths = new ArrayList<>(batch.getEvents().size());
            for (ConfigUpdateEvent event : batch.getEvents()) {
                if (configName.compareTo(event.getHeader().getConfigName()) != 0) {
                    throw new ConfigurationException(String.format(
                            "Invalid Event batch : Multiple configurations specified. [configName=%s]",
                            configName));
                }
                processEvent(event);
                updatePaths.add(event.getPath());
            }
            ZConfigClientEnv.clientEnv().getConfigurationManager()
                    .applyConfigurationUpdates(configName, updatePaths);
        } catch (Exception e) {
            LogUtils.error(getClass(), String.format(
                    "Update failed to configuration [name=%s][transaction=%s] : %s",
                    batch.getHeader().getConfigName(),
                    batch.getHeader().getTransactionId(), e.getLocalizedMessage()));
            throw new ConfigurationException(e);
        }
    }

    /**
     * Process and apply the specified configuration update event.
     *
     * @param event - Update Event.
     * @throws ConfigurationException
     */
    private void processEvent(@Nonnull ConfigUpdateEvent event) throws
            ConfigurationException {
        Preconditions.checkArgument(event != null);
        try {
            ConfigurationManager manager =
                    ZConfigClientEnv.clientEnv().getConfigurationManager();
            Configuration config = manager.getWithLock(event.getHeader().getConfigName());
            if (config == null) {
                LogUtils.debug(getClass(),
                        String.format(
                                "Configuration not loaded. [name=%s]",
                                event.getHeader().getConfigName()));
                return;
            }
            try {
                Version prev = Version.parse(event.getHeader().getPreVersion());
                Version curr = Version.parse(event.getHeader().getUpdatedVersion());
                if (!config.getVersion().equals(prev)) {
                    throw new ConfigurationException(String.format(
                            "Invalid Sync state: Event version out of sync. [expected=%s][actual=%s]",
                            config.getVersion().toString(), prev.toString()));
                }
                AbstractConfigNode node = config.find(event.getPath());
                if (node == null) {
                    throw new ConfigurationException(String.format(
                            "Invalid Sync state: Specified node not found. [config=%s][path=%s]",
                            event.getHeader().getConfigName(), event.getPath()));
                }
                switch (event.getEventType()) {
                    case Add:
                        processAddEvent(event, node, config);
                        break;
                    case Update:
                        processUpdateEvent(event, node, config);
                        break;
                    case Remove:
                        processDeleteEvent(event, node, config);
                        break;
                }
                LogUtils.info(getClass(), String.format(
                        "Updated configuration : [name=%s][version=%s]",
                        event.getHeader().getConfigName(), curr.toString()));
            } finally {
                if (!manager.releaseLock(event.getHeader().getConfigName())) {
                    LogUtils.warn(getClass(), String.format(
                            "Configuration update lock release failed. [config=%s]",
                            event.getHeader().getConfigName()));
                }
            }
        } catch (Exception e) {
            LogUtils.error(getClass(), e);
            throw new ConfigurationException(e);
        }
    }

    /**
     * Process the Add event.
     *
     * @param event         - Event handle.
     * @param parent        - Parent configuration node.
     * @param configuration - Configuration instance.
     * @throws ConfigurationException
     */
    private void processAddEvent(ConfigUpdateEvent event, AbstractConfigNode parent,
                                 Configuration configuration)
            throws ConfigurationException {
        LogUtils.debug(getClass(),
                String.format("Applying Add change. [config=%s][path=%s]",
                        event.getHeader().getConfigName(),
                        parent.getAbsolutePath()));
        if (parent instanceof ConfigPathNode) {
            ConfigPathNode cp = (ConfigPathNode) parent;
            AbstractConfigNode cnode = cp.getChildNode(event.getValue().getName());
            if (cnode != null) {
                throw new ConfigurationException(String.format(
                        "Add failed : Node already exists. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), cnode.getAbsolutePath(),
                        event.getValue().getName()));
            }
            cp.addChildNode(event.getValue());
        } else if (parent instanceof ConfigKeyValueNode) {
            ConfigKeyValueNode cp = (ConfigKeyValueNode) parent;
            if (cp.hasKey(event.getValue().getName())) {
                throw new ConfigurationException(String.format(
                        "Add failed : Node already exists. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), cp.getAbsolutePath(),
                        event.getValue().getName()));
            }
            cp.addKeyValue(event.getValue().getName(), event.getValue().getValue());
        } else if (parent instanceof ConfigListValueNode) {
            ConfigListValueNode cp = (ConfigListValueNode) parent;
            ConfigValueNode vn = cp.getValue(event.getValue().getName());
            if (vn != null) {
                throw new ConfigurationException(String.format(
                        "Add failed : Node already exists. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), vn.getAbsolutePath(),
                        event.getValue().getName()));
            }
            cp.addValue(event.getValue());
        } else throw new ConfigurationException(
                String.format("Add event not supported : [config=%s][path=%s]",
                        event.getHeader().getConfigName(), parent.getAbsolutePath()));
    }

    /**
     * Process the Update event.
     *
     * @param event         - Event handle.
     * @param parent        - Parent configuration node.
     * @param configuration - Configuration instance.
     * @throws ConfigurationException
     */
    private void processUpdateEvent(ConfigUpdateEvent event,
                                    AbstractConfigNode parent,
                                    Configuration configuration)
            throws ConfigurationException {
        if (parent instanceof ConfigPathNode) {
            ConfigPathNode cp = (ConfigPathNode) parent;
            AbstractConfigNode cnode = cp.getChildNode(event.getValue().getName());
            if (cnode == null) {
                throw new ConfigurationException(String.format(
                        "Add failed : Node to update not found. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), cp.getAbsolutePath(),
                        event.getValue().getName()));
            }
            if (!(cnode instanceof ConfigValueNode)) {
                throw new ConfigurationException(String.format(
                        "Add failed : Cannot update node. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), cp.getAbsolutePath(),
                        event.getValue().getName()));
            }
            ((ConfigValueNode) cnode).setValue(event.getValue().getValue());
        } else if (parent instanceof ConfigListValueNode) {
            ConfigListValueNode cp = (ConfigListValueNode) parent;
            ConfigValueNode vn = cp.getValue(event.getValue().getName());
            if (vn == null) {
                throw new ConfigurationException(String.format(
                        "Add failed : Node not found. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), cp.getAbsolutePath(),
                        event.getValue().getName()));
            }
            vn.setValue(event.getValue().getValue());
        } else if (parent instanceof ConfigKeyValueNode) {
            ConfigKeyValueNode cp = (ConfigKeyValueNode) parent;
            if (!cp.hasKey(event.getValue().getName())) {
                throw new ConfigurationException(String.format(
                        "Add failed : Key/Value to update not found. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), cp.getAbsolutePath(),
                        event.getValue().getName()));
            }
            cp.addKeyValue(event.getValue().getName(), event.getValue().getValue());
        } else throw new ConfigurationException(
                String.format("Add event not supported : [config=%s][path=%s]",
                        event.getHeader().getConfigName(), parent.getAbsolutePath()));
    }

    /**
     * Process the Delete event.
     *
     * @param event         - Event handle.
     * @param parent        - Parent configuration node.
     * @param configuration - Configuration instance.
     * @throws ConfigurationException
     */
    private void processDeleteEvent(ConfigUpdateEvent event,
                                    AbstractConfigNode parent,
                                    Configuration configuration)
            throws ConfigurationException {
        if (parent instanceof ConfigPathNode) {
            ConfigPathNode cp = (ConfigPathNode) parent;
            AbstractConfigNode cnode = cp.getChildNode(event.getValue().getName());
            if (cnode == null) {
                throw new ConfigurationException(String.format(
                        "Add failed : Node to update not found. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), cp.getAbsolutePath(),
                        event.getValue().getName()));
            }
            if (!(cnode instanceof ConfigValueNode)) {
                throw new ConfigurationException(String.format(
                        "Add failed : Cannot update node. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), cp.getAbsolutePath(),
                        event.getValue().getName()));
            }
            cp.removeChildNode(event.getValue().getName());
        } else if (parent instanceof ConfigListValueNode) {
            ConfigListValueNode cp = (ConfigListValueNode) parent;
            ConfigValueNode vn = cp.getValue(event.getValue().getName());
            if (vn == null) {
                throw new ConfigurationException(String.format(
                        "Add failed : Node not found. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), cp.getAbsolutePath(),
                        event.getValue().getName()));
            }
            cp.removeValue(vn);
        } else if (parent instanceof ConfigKeyValueNode) {
            ConfigKeyValueNode cp = (ConfigKeyValueNode) parent;
            if (!cp.hasKey(event.getValue().getName())) {
                throw new ConfigurationException(String.format(
                        "Add failed : Key/Value to update not found. [config=%s][path=%s][name=%s]",
                        event.getHeader().getConfigName(), cp.getAbsolutePath(),
                        event.getValue().getName()));
            }
            cp.removeKeyValue(event.getValue().getName());
        } else throw new ConfigurationException(
                String.format("Add event not supported : [config=%s][path=%s]",
                        event.getHeader().getConfigName(), parent.getAbsolutePath()));
    }
}
