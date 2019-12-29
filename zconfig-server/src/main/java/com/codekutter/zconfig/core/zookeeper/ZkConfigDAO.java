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
 * Date: 17/2/19 9:32 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core.zookeeper;

import com.codekutter.common.model.ModifiedBy;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.IUniqueIDGenerator;
import com.codekutter.zconfig.common.ZConfigCoreEnv;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.model.nodes.*;
import com.codekutter.zconfig.core.IConfigDAO;
import com.codekutter.zconfig.core.PersistenceException;
import com.codekutter.zconfig.core.ServiceEnvException;
import com.codekutter.zconfig.core.model.Application;
import com.codekutter.zconfig.core.model.ApplicationGroup;
import com.codekutter.zconfig.core.model.PersistedConfigNode;
import com.codekutter.zconfig.core.model.PersistedConfigPathNode;
import com.codekutter.zconfig.core.model.nodes.PersistedConfigListValueNode;
import com.codekutter.zconfig.core.model.nodes.PersistedConfigMapNode;
import com.codekutter.zconfig.core.model.nodes.PersistedConfigValueNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nonnull;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Access Object to read/update configuration data from ZooKeeper.
 */
public class ZkConfigDAO implements IConfigDAO {

    /**
     * Create/Update the Application Group passed to ZooKeeper.
     *
     * @param client - Curator Client handle.
     * @param group  - Application Group instance.
     * @param user   - User Principal
     * @return - Application Group instance.
     * @throws PersistenceException
     */
    @Override
    public ApplicationGroup saveApplicationGroup(@Nonnull CuratorFramework client,
                                                 @Nonnull ApplicationGroup group,
                                                 @Nonnull Principal user)
            throws PersistenceException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group.getName()));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group.getDescription()));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group.getChannelName()));
        try {
            ModifiedBy modifiedBy = new ModifiedBy(user.getName());

            String zkPath = ZkUtils.getZkPath(group);
            Stat stat = client.checkExists().forPath(zkPath);
            if (stat == null) {
                zkPath = client.create().creatingParentsIfNeeded().forPath(zkPath);
                group.setOwner(modifiedBy);
            } else {
                byte[] data = client.getData().forPath(zkPath);
                if (data == null || data.length == 0) {
                    group.setOwner(modifiedBy);
                } else {
                    String json = new String(data);
                    if (Strings.isNullOrEmpty(json)) {
                        throw new PersistenceException(
                                "Invalid Application Group : NULL/empty data returned.");
                    }
                    ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
                    ApplicationGroup nGroup =
                            mapper.readValue(json, ApplicationGroup.class);
                    if (group.getId().compareTo(nGroup.getId()) != 0) {
                        throw new PersistenceException(String.format(
                                "Error Updating Application Group : ID mismatch. [expected=%s][actual=%s]",
                                group.getId(), nGroup.getId()));
                    }
                }
            }
            group.setUpdated(modifiedBy);
            ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
            String json = mapper.writeValueAsString(group);
            client.setData().forPath(zkPath, json.getBytes());

            return group;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Create/Update the Application Group passed to ZooKeeper.
     *
     * @param client      - Curator Client handle.
     * @param application - Application Group instance.
     * @param user        - User Principal
     * @return - Application Group instance.
     * @throws PersistenceException
     */
    @Override
    public Application saveApplication(@Nonnull CuratorFramework client,
                                       @Nonnull Application application,
                                       @Nonnull Principal user)
            throws PersistenceException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(application.getName()));
        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(application.getDescription()));
        try {
            ModifiedBy modifiedBy = new ModifiedBy(user.getName());

            String zkPath = ZkUtils.getZkPath(application);
            Stat stat = client.checkExists().forPath(zkPath);
            if (stat == null) {
                application.setOwner(modifiedBy);
                zkPath = client.create().creatingParentsIfNeeded().forPath(zkPath);
            } else {
                byte[] data = client.getData().forPath(zkPath);
                if (data == null || data.length == 0) {
                    application.setOwner(modifiedBy);
                } else {
                    String json = new String(data);
                    if (Strings.isNullOrEmpty(json)) {
                        throw new PersistenceException(
                                "Invalid Application Group : NULL/empty data returned.");
                    }
                    ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
                    Application nGroup =
                            mapper.readValue(json, Application.class);
                    if (application.getId().compareTo(nGroup.getId()) != 0) {
                        throw new PersistenceException(String.format(
                                "Error Updating Application Group : ID mismatch. [expected=%s][actual=%s]",
                                application.getId(), nGroup.getId()));
                    }
                }
            }
            application.setUpdated(modifiedBy);
            ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
            String json = mapper.writeValueAsString(application);
            client.setData().forPath(zkPath, json.getBytes());

            return application;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Save the specified configuration header into ZooKeeper.
     *
     * @param client        - Curator Client handle.
     * @param configuration - Configuration instance.
     * @param version       - Updated Version.
     * @param user          - User Principle
     * @return - Updated Config Node.
     * @throws PersistenceException
     */
    @Override
    public PersistedConfigNode saveConfigHeader(@Nonnull CuratorFramework client,
                                                @Nonnull
                                                        Configuration configuration,
                                                @Nonnull Version version,
                                                @Nonnull Principal user)
            throws PersistenceException {
        try {
            ApplicationGroup group = readApplicationGroup(client, configuration
                    .getApplicationGroup());
            if (group == null) {
                throw new PersistenceException(
                        String.format("Application Group not found. [group=%s]",
                                configuration.getApplicationGroup()));
            }
            Application application =
                    readApplication(client, group, configuration.getApplication());
            if (application == null) {
                throw new PersistenceException(
                        String.format("Application not found. [application=%s]",
                                configuration.getApplication()));
            }
            ModifiedBy modifiedBy = new ModifiedBy(user.getName());
            String zkPath = ZkUtils.getZkPath(configuration);
            Stat stat = client.checkExists().forPath(zkPath);
            PersistedConfigNode configNode = null;
            if (stat == null) {
                configNode = new PersistedConfigNode();
                setupConfigHeaderNode(configNode, configuration, application,
                        modifiedBy);
                zkPath = client.create().creatingParentsIfNeeded().forPath(zkPath);
            } else {
                byte[] data = client.getData().forPath(zkPath);
                if (data == null || data.length == 0) {
                    configNode = new PersistedConfigNode();
                    setupConfigHeaderNode(configNode, configuration, application,
                            modifiedBy);
                } else {
                    ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
                    configNode = mapper.readValue(data, PersistedConfigNode.class);
                    if (!configuration.getVersion()
                            .equals(configNode.getCurrentVersion())) {
                        throw new PersistenceException(String.format(
                                "Updating Stale Version : [expected=%s][actual=%s]",
                                configNode.getCurrentVersion().toString(),
                                configuration.getVersion().toString()));
                    }
                    configNode.setDescription(configuration.getDescription());
                    configNode.setSyncMode(configuration.getSyncMode());
                }
            }
            configNode.setUpdated(modifiedBy);
            configNode.setCurrentVersion(version);

            ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
            String json = mapper.writeValueAsString(configNode);

            client.setData().forPath(zkPath, json.getBytes());

            return configNode;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Setup the configuration header node.
     *
     * @param configNode    - Configuration Header node.
     * @param configuration - Configuration instance.
     * @param application   - Parent application.
     * @param modifiedBy    - Modified By Info.
     * @throws ServiceEnvException
     */
    private void setupConfigHeaderNode(PersistedConfigNode configNode,
                                       Configuration configuration,
                                       Application application,
                                       ModifiedBy modifiedBy)
            throws ServiceEnvException {
        try {
            configNode.setId(ZConfigCoreEnv.coreEnv().getIdGenerator()
                    .generateStringId(null));
            configNode.setApplication(application);
            configNode.setName(configuration.getName());
            configNode.setDescription(configuration.getDescription());
            configNode.setSyncMode(configuration.getSyncMode());
            configNode.setOwner(modifiedBy);
        } catch (Exception ex) {
            throw new ServiceEnvException(ex);
        }
    }

    /**
     * Save or Update the passed configuration node.
     *
     * @param client     - Curator Client handle.
     * @param node       - Configuration node to save/update.
     * @param configNode - ZK Configuration Node
     * @param version    - Updated Version
     * @param user       - User Principal
     * @return - Created/Updated Config Path node.
     * @throws PersistenceException
     */
    @Override
    public PersistedConfigPathNode saveConfigNode(@Nonnull CuratorFramework client,
                                                  @Nonnull AbstractConfigNode node,
                                                  @Nonnull
                                                          PersistedConfigNode configNode,
                                                  @Nonnull Version version,
                                                  @Nonnull Principal user)
            throws PersistenceException {
        try {
            String path = node.getAbsolutePath();
            String zkPath = ZkUtils.getZkPath(configNode, path);
            Stat stat = client.checkExists().forPath(zkPath);
            if (node instanceof ConfigValueNode) {
                return saveValueConfigNode(client, (ConfigValueNode) node,
                        configNode,
                        user, version, zkPath, stat);
            } else if (node instanceof ConfigListValueNode) {
                return saveValueListConfigNode(client, (ConfigListValueNode) node,
                        configNode,
                        user, version, zkPath, stat);
            } else if (node instanceof ConfigParametersNode) {
                return saveKeyValueConfigNode(client, (ConfigParametersNode) node,
                        configNode,
                        user, version, zkPath, stat);
            } else if (node instanceof ConfigPropertiesNode) {
                return saveKeyValueConfigNode(client, (ConfigPropertiesNode) node,
                        configNode,
                        user, version, zkPath, stat);
            }
            return null;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Save or Update the passed configuration value node.
     *
     * @param client     - Curator Client handle.
     * @param node       - Configuration node to save/update.
     * @param configNode - ZK Configuration Node
     * @param version    - Updated Version
     * @param user       - User Principal
     * @param zkPath     - ZooKeeper node path.
     * @param stat       - ZK Stat response.
     * @return - Created/Updated Config Path node.
     * @throws PersistenceException
     */
    private PersistedConfigValueNode saveValueConfigNode(
            @Nonnull CuratorFramework client,
            @Nonnull ConfigValueNode node,
            @Nonnull
                    PersistedConfigNode configNode,
            @Nonnull Principal user,
            @Nonnull Version version,
            String zkPath,
            Stat stat) throws PersistenceException {
        try {
            ModifiedBy modifiedBy = new ModifiedBy(user.getName());

            PersistedConfigValueNode zkNode = null;
            if (stat == null) {
                zkNode = new PersistedConfigValueNode();
                setupNewPathNode(zkNode, node, modifiedBy, configNode,
                        version);
                zkNode.setValue(node.getValue());
                String path = zkNode.getAbsolutePath();
                path = client.create().creatingParentsIfNeeded().forPath(path);
            } else {
                byte[] data = client.getData().forPath(zkPath);
                if (data == null || data.length <= 0) {
                    zkNode = new PersistedConfigValueNode();
                    setupNewPathNode(zkNode, node, modifiedBy, configNode,
                            version);
                    zkNode.setValue(node.getValue());
                } else {
                    String json = new String(data);
                    ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
                    zkNode = mapper.readValue(json, PersistedConfigValueNode.class);
                    if (configNode.getCurrentVersion()
                            .compareMinorVersion(zkNode.getNodeVersion()) <
                            0) {
                        throw new PersistenceException(String.format(
                                "Update Failed : Passed node version is stale. [expected=%s][actual=%s]",
                                configNode.getCurrentVersion().toString(),
                                zkNode.getNodeVersion().toString()));
                    }
                    zkNode.setValue(node.getValue());
                    zkNode.setNodeVersion(version);
                    zkNode.setUpdated(modifiedBy);
                }
            }
            String path = zkNode.getAbsolutePath();
            String json =
                    ZConfigCoreEnv.coreEnv().getJsonMapper()
                            .writeValueAsString(zkNode);
            client.setData().forPath(path, json.getBytes());

            return zkNode;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }


    /**
     * Setup a new ZK Path node.
     *
     * @param zkNode     - ZK path
     * @param node       - Configuration node.
     * @param owner      - User Principal
     * @param configNode - ZK Configuration Node
     * @param version    - Updated Version
     * @throws ServiceEnvException
     */
    private void setupNewPathNode(PersistedConfigPathNode zkNode,
                                  AbstractConfigNode node, ModifiedBy owner,
                                  PersistedConfigNode configNode, Version version)
            throws
            ServiceEnvException {
        try {
            zkNode = new PersistedConfigValueNode();
            IUniqueIDGenerator idGenerator =
                    ZConfigCoreEnv.coreEnv().getIdGenerator();
            zkNode.setId(idGenerator.generateStringId(null));
            zkNode.setName(node.getName());
            String desc = ConfigUtils.getDescription(node);
            if (!Strings.isNullOrEmpty(desc))
                zkNode.setDescription(desc);
            zkNode.setParent(configNode);
            zkNode.setOwner(owner);
            zkNode.setUpdated(owner);
            zkNode.setNodeVersion(version);
        } catch (Exception ex) {
            throw new ServiceEnvException(ex);
        }
    }

    /**
     * Save or Update the passed configuration value list node.
     *
     * @param client     - Curator Client handle.
     * @param node       - Configuration node to save/update.
     * @param configNode - ZK Configuration Node
     * @param user       - User Principal
     * @param version    - Updated Version
     * @param zkPath     - ZooKeeper node path.
     * @param stat       - ZK Stat response.
     * @return - Created/Updated Config Path node.
     * @throws PersistenceException
     */
    private PersistedConfigListValueNode saveValueListConfigNode(
            @Nonnull CuratorFramework client,
            @Nonnull ConfigListValueNode node,
            @Nonnull PersistedConfigNode configNode,
            @Nonnull Principal user,
            @Nonnull Version version,
            String zkPath,
            Stat stat) throws PersistenceException {
        try {
            ModifiedBy modifiedBy = new ModifiedBy(user.getName());
            List<String> values = null;
            if (node.getValues() != null && !node.getValues().isEmpty()) {
                values = new ArrayList<>(node.size());
                for (ConfigValueNode vn : node.getValues()) {
                    values.add(vn.getValue());
                }
            }
            PersistedConfigListValueNode zkNode = null;
            if (stat == null) {
                zkNode = new PersistedConfigListValueNode();
                setupNewPathNode(zkNode, node, modifiedBy, configNode,
                        version);
                zkNode.setValues(values);
                String path = zkNode.getAbsolutePath();
                path = client.create().creatingParentsIfNeeded().forPath(path);
            } else {
                byte[] data = client.getData().forPath(zkPath);
                if (data == null || data.length <= 0) {
                    zkNode = new PersistedConfigListValueNode();
                    setupNewPathNode(zkNode, node, modifiedBy, configNode,
                            version);
                    zkNode.setValues(values);
                } else {
                    String json = new String(data);
                    ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
                    zkNode = mapper.readValue(json,
                            PersistedConfigListValueNode.class);
                    if (configNode.getCurrentVersion()
                            .compareMinorVersion(zkNode.getNodeVersion()) <
                            0) {
                        throw new PersistenceException(String.format(
                                "Update Failed : Passed node version is stale. [expected=%s][actual=%s]",
                                configNode.getCurrentVersion().toString(),
                                zkNode.getNodeVersion().toString()));
                    }
                    zkNode.setValues(values);
                    zkNode.setNodeVersion(version);
                    zkNode.setUpdated(modifiedBy);
                }
            }
            String path = zkNode.getAbsolutePath();
            String json =
                    ZConfigCoreEnv.coreEnv().getJsonMapper()
                            .writeValueAsString(zkNode);
            client.setData().forPath(path, json.getBytes());

            return zkNode;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Save or Update the passed configuration parameters node.
     *
     * @param client     - Curator Client handle.
     * @param node       - Configuration node to save/update.
     * @param configNode - ZK Configuration Node
     * @param user       - User Principal
     * @param version    - Updated Version
     * @param zkPath     - ZooKeeper node path.
     * @param stat       - ZK Stat response.
     * @return - Created/Updated Config Path node.
     * @throws PersistenceException
     */
    private PersistedConfigMapNode saveKeyValueConfigNode(
            @Nonnull CuratorFramework client,
            @Nonnull ConfigKeyValueNode node,
            @Nonnull PersistedConfigNode configNode,
            @Nonnull Principal user,
            @Nonnull Version version,
            String zkPath,
            Stat stat) throws PersistenceException {
        try {
            ModifiedBy modifiedBy = new ModifiedBy(user.getName());

            PersistedConfigMapNode zkNode = null;
            if (stat == null) {
                zkNode = new PersistedConfigMapNode();
                setupNewPathNode(zkNode, node, modifiedBy, configNode,
                        version);
                zkNode.setMapFrom(node.getKeyValues());
                String path = zkNode.getAbsolutePath();
                path = client.create().creatingParentsIfNeeded().forPath(path);
            } else {
                byte[] data = client.getData().forPath(zkPath);
                if (data == null || data.length <= 0) {
                    zkNode = new PersistedConfigMapNode();
                    setupNewPathNode(zkNode, node, modifiedBy, configNode,
                            version);
                    zkNode.setMapFrom(node.getKeyValues());
                } else {
                    String json = new String(data);
                    ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
                    zkNode = mapper.readValue(json, PersistedConfigMapNode.class);
                    if (configNode.getCurrentVersion()
                            .compareMinorVersion(zkNode.getNodeVersion()) <
                            0) {
                        throw new PersistenceException(String.format(
                                "Update Failed : Passed node version is stale. [expected=%s][actual=%s]",
                                configNode.getCurrentVersion().toString(),
                                zkNode.getNodeVersion().toString()));
                    }
                    zkNode.setMapFrom(node.getKeyValues());
                    zkNode.setNodeVersion(version);
                    zkNode.setUpdated(modifiedBy);
                }
            }
            String path = zkNode.getAbsolutePath();
            String json =
                    ZConfigCoreEnv.coreEnv().getJsonMapper()
                            .writeValueAsString(zkNode);
            client.setData().forPath(path, json.getBytes());

            return zkNode;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Read the Config Path node for the specified node path.
     *
     * @param client     - Curator client handle.
     * @param configNode - Configuration node.
     * @param nodePath   - Node Path to read from.
     * @return - Read Path Config node.
     * @throws PersistenceException
     */
    @Override
    public PersistedConfigPathNode readConfigNode(
            @Nonnull CuratorFramework client,
            @Nonnull PersistedConfigNode configNode,
            String nodePath) throws PersistenceException {
        try {
            String zkPath = ZkUtils.getZkPath(configNode, nodePath);
            Stat stat = client.checkExists().forPath(zkPath);
            if (stat != null) {
                byte[] data = client.getData().forPath(zkPath);
                String json = new String(data);
                if (!Strings.isNullOrEmpty(json)) {
                    ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
                    return mapper.readValue(json, PersistedConfigPathNode.class);
                }
            }
            return null;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Delete the Config Path node for the specified node path.
     *
     * @param client     - Curator client handle.
     * @param configNode - Configuration node.
     * @param nodePath   - Node Path to read from.
     * @return - Is Deleted?
     * @throws PersistenceException
     */
    @Override
    public boolean deleteConfigNode(@Nonnull CuratorFramework client,
                                    @Nonnull PersistedConfigNode configNode,
                                    String nodePath) throws PersistenceException {
        try {
            String zkPath = ZkUtils.getZkPath(configNode, nodePath);
            Stat stat = client.checkExists().forPath(zkPath);
            if (stat != null) {
                client.delete().forPath(zkPath);
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Get all the child nodes for this path.
     *
     * @param client     - Curator client handle.
     * @param configNode - Configuration node.
     * @param nodePath   - Node Path to read from.
     * @return - List of child nodes (String)
     * @throws PersistenceException
     */
    @Override
    public List<String> getChildren(@Nonnull CuratorFramework client,
                                    @Nonnull PersistedConfigNode configNode,
                                    String nodePath) throws PersistenceException {
        try {
            String zkPath = ZkUtils.getZkPath(configNode, nodePath);
            Stat stat = client.checkExists().forPath(zkPath);
            if (stat != null) {
                return client.getChildren().forPath(zkPath);
            }
            return null;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Read an Application Group instance specified by the group name.
     *
     * @param client    - Curator client handle.
     * @param groupName - Application Group name.
     * @return - Application Group instance.
     * @throws PersistenceException
     */
    @Override
    public ApplicationGroup readApplicationGroup(@Nonnull CuratorFramework client,
                                                 @Nonnull String groupName)
            throws PersistenceException {
        try {
            String zkPath = ZkUtils.getZkPath(groupName);
            Stat stat = client.checkExists().forPath(zkPath);
            if (stat != null) {
                byte[] data = client.getData().forPath(zkPath);
                if (data != null && data.length > 0) {
                    ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
                    return mapper.readValue(data, ApplicationGroup.class);
                }
            }
            return null;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Read an Application Group instance specified by the group name.
     *
     * @param client - Curator client handle.
     * @param group  - Application Group
     * @param name   - Application name.
     * @return - Application  instance.
     * @throws PersistenceException
     */
    @Override
    public Application readApplication(@Nonnull CuratorFramework client,
                                       @Nonnull ApplicationGroup group,
                                       @Nonnull String name)
            throws PersistenceException {
        try {
            String zkPath = ZkUtils.getZkPath(group, name);
            Stat stat = client.checkExists().forPath(zkPath);
            if (stat != null) {
                byte[] data = client.getData().forPath(zkPath);
                if (data != null && data.length > 0) {
                    ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
                    return mapper.readValue(data, Application.class);
                }
            }
            return null;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Read an Configuration Header instance specified by the group name.
     *
     * @param client      - Curator client handle.
     * @param application - Application
     * @param name        - Configuration name.
     * @param version     - Configuration Version
     * @return - Application  instance.
     * @throws PersistenceException
     */
    @Override
    public PersistedConfigNode readConfigHeader(@Nonnull CuratorFramework client,
                                                @Nonnull Application application,
                                                @Nonnull String name,
                                                @Nonnull Version version)
            throws PersistenceException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        try {
            String zkPath = ZkUtils.getZkPath(application, name, version);
            Stat stat = client.checkExists().forPath(zkPath);
            if (stat != null) {
                byte[] data = client.getData().forPath(zkPath);
                if (data != null && data.length > 0) {
                    ObjectMapper mapper = ZConfigCoreEnv.coreEnv().getJsonMapper();
                    return mapper.readValue(data, PersistedConfigNode.class);
                }
            }
            return null;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

}
