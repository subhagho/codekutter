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
 * Date: 18/2/19 8:16 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core;

import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.core.model.Application;
import com.codekutter.zconfig.core.model.ApplicationGroup;
import com.codekutter.zconfig.core.model.PersistedConfigNode;
import com.codekutter.zconfig.core.model.PersistedConfigPathNode;
import org.apache.curator.framework.CuratorFramework;

import javax.annotation.Nonnull;
import java.security.Principal;
import java.util.List;

/**
 * Data Access Object interface to be implemented to read/update configuration data.
 */
public interface IConfigDAO {
    /**
     * Create/Update the Application Group passed to ZooKeeper.
     *
     * @param client - Curator Client handle.
     * @param group  - Application Group instance.
     * @param user   - User Principal
     * @return - Application Group instance.
     * @throws PersistenceException
     */
    ApplicationGroup saveApplicationGroup(@Nonnull CuratorFramework client,
                                          @Nonnull ApplicationGroup group,
                                          @Nonnull Principal user)
            throws PersistenceException;

    /**
     * Create/Update the Application Group passed to ZooKeeper.
     *
     * @param client      - Curator Client handle.
     * @param application - Application Group instance.
     * @param user        - User Principal
     * @return - Application Group instance.
     * @throws PersistenceException
     */
    Application saveApplication(@Nonnull CuratorFramework client,
                                @Nonnull Application application,
                                @Nonnull Principal user)
            throws PersistenceException;

    /**
     * Create/Update the header for the specified configuration.
     *
     * @param client - Curator Client handle.
     * @param configuration - Configuration to save header for
     * @param version - Updated Version
     * @param user - Invoking User
     * @return - Persisted Configuration node.
     * @throws PersistenceException
     */
    PersistedConfigNode saveConfigHeader(@Nonnull CuratorFramework client,
                                         @Nonnull
                                                 Configuration configuration,
                                         @Nonnull Version version,
                                         @Nonnull Principal user)
            throws PersistenceException;

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
    PersistedConfigPathNode saveConfigNode(@Nonnull CuratorFramework client,
                                           @Nonnull AbstractConfigNode node,
                                           @Nonnull
                                                   PersistedConfigNode configNode,
                                           @Nonnull Version version,
                                           @Nonnull Principal user)
            throws PersistenceException;

    /**
     * Read the Config Path node for the specified node path.
     *
     * @param client     - Curator client handle.
     * @param configNode - Configuration node.
     * @param nodePath   - Node Path to read from.
     * @return - Read Path Config node.
     * @throws PersistenceException
     */
    PersistedConfigPathNode readConfigNode(
            @Nonnull CuratorFramework client,
            @Nonnull PersistedConfigNode configNode,
            String nodePath) throws PersistenceException;

    /**
     * Delete the Config Path node for the specified node path.
     *
     * @param client     - Curator client handle.
     * @param configNode - Configuration node.
     * @param nodePath   - Node Path to read from.
     * @return - Is Deleted?
     * @throws PersistenceException
     */
    boolean deleteConfigNode(@Nonnull CuratorFramework client,
                             @Nonnull PersistedConfigNode configNode,
                             String nodePath) throws PersistenceException;

    /**
     * Get all the child nodes for this path.
     *
     * @param client     - Curator client handle.
     * @param configNode - Configuration node.
     * @param nodePath   - Node Path to read from.
     * @return - List of child nodes (String)
     * @throws PersistenceException
     */
    List<String> getChildren(@Nonnull CuratorFramework client,
                             @Nonnull PersistedConfigNode configNode,
                             String nodePath) throws PersistenceException;

    /**
     * Read an Application Group instance specified by the group name.
     *
     * @param client    - Curator client handle.
     * @param groupName - Application Group name.
     * @return - Application Group instance.
     * @throws PersistenceException
     */
    ApplicationGroup readApplicationGroup(@Nonnull CuratorFramework client,
                                          @Nonnull String groupName)
            throws PersistenceException;

    /**
     * Read an Application Group instance specified by the group name.
     *
     * @param client - Curator client handle.
     * @param group  - Application Group
     * @param name   - Application name.
     * @return - Application  instance.
     * @throws PersistenceException
     */
    Application readApplication(@Nonnull CuratorFramework client,
                                @Nonnull ApplicationGroup group,
                                @Nonnull String name)
            throws PersistenceException;

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
    PersistedConfigNode readConfigHeader(@Nonnull CuratorFramework client,
                                         @Nonnull Application application,
                                         @Nonnull String name,
                                         @Nonnull Version version)
            throws PersistenceException;
}
