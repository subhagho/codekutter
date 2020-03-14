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
 * Date: 16/2/19 12:14 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core.zookeeper;

import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ZConfigCoreEnv;
import com.codekutter.zconfig.common.ZConfigCoreInstance;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.core.PersistenceException;
import com.codekutter.zconfig.core.model.Application;
import com.codekutter.zconfig.core.model.ApplicationGroup;
import com.codekutter.zconfig.core.model.IZkNode;
import com.codekutter.zconfig.core.model.PersistedConfigNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;

import javax.annotation.Nonnull;

/**
 * Helper class for ZooKeeper.
 */
public class ZkUtils {
    /**
     * Default path for creating configuration locks.
     */
    private static final String ZK_LOCK_PATH = "__LOCKS__";
    /**
     * System Root lock path.
     */
    private static final String ZK_ROOT_LOCK = "__ROOT_LOCK__";
    /**
     * Default retry sleep interval.
     */
    private static final int DEFAULT_RETRY_SLEEP = 1000;
    /**
     * ZooKeeper Root path for this server.
     */
    private static final String SERVER_ROOT_PATH = "/ZCONFIG-SERVER";

    /**
     * Get a new instance of the Curator Client. Method will start() the client.
     *
     * @return - Curator Framework Client.
     * @throws PersistenceException
     */
    public static final CuratorFramework getZkClient() throws PersistenceException {
        try {
            ZkConnectionConfig config =
                    ZConfigCoreEnv.coreEnv().getZkConnectionConfig();
            if (config == null) {
                throw new PersistenceException(
                        "ZooKeeper Connection configuration not set.");
            }
            RetryPolicy retryPolicy = null;
            if (!Strings.isNullOrEmpty(config.getRetryClass())) {
                LogUtils.debug(ZkUtils.class,
                        String.format("Using Retry implemenation : %s",
                                config.getRetryClass()));
                Class<?> type = Class.forName(config.getRetryClass());
                if (type.equals(ExponentialBackoffRetry.class)) {
                    if (config.getSleepTime() <= 0 || config.getMaxRetries() < 0) {
                        throw new PersistenceException(String.format(
                                "Missing Retry Parameter(s) : [type=%s]",
                                config.getRetryClass()));
                    }
                    retryPolicy = new ExponentialBackoffRetry(config.getSleepTime(),
                            config.getMaxRetries());
                }
            }
            if (retryPolicy == null) {
                retryPolicy = new RetryOneTime(DEFAULT_RETRY_SLEEP);
            }
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(config.getConnectionString()).retryPolicy(retryPolicy).build();
            client.start();
            return client;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Get the ZooKeeper root path for this server.
     *
     * @return - ZooKeeper root path.
     * @throws PersistenceException
     */
    public static final String getServerRootPath() throws PersistenceException {
        try {
            ZkConnectionConfig config =
                    ZConfigCoreEnv.coreEnv().getZkConnectionConfig();
            if (config == null) {
                throw new PersistenceException(
                        "ZooKeeper Connection configuration not set.");
            }
            String rp = config.getRootPath();
            if (!Strings.isNullOrEmpty(rp)) {
                if (!rp.startsWith("/")) {
                    rp = String.format("/%s", rp);
                }
                ZConfigCoreInstance instance = ZConfigCoreEnv.coreEnv().getInstance();
                return String
                        .format("%s%s/%s", SERVER_ROOT_PATH, rp,
                                instance.getApplicationName());
            } else {
                ZConfigCoreInstance instance = ZConfigCoreEnv.coreEnv().getInstance();
                return String
                        .format("%s/%s", SERVER_ROOT_PATH, instance.getApplicationName());
            }
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Create a new instance of a distributed ZooKeeper lock with the specified name.
     *
     * @param client - Curator Framework client handle.
     * @param name   - Lock name.
     * @return - Lock instance.
     * @throws PersistenceException
     */
    public static final InterProcessMutex getZkLock(
            @Nonnull CuratorFramework client,
            @Nonnull String name)
            throws PersistenceException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        String path =
                String.format("%s/%s/%s", getServerRootPath(), ZK_LOCK_PATH, name);

        LogUtils.debug(ZkUtils.class,
                String.format("Getting ZK Lock : [lock=%s]...", path));

        return new InterProcessMutex(client, path);
    }

    /**
     * Get the lock to the ZooKeeper root.
     *
     * @param client - Curator Framework client handle.
     * @return - Lock instance.
     * @throws PersistenceException
     */
    public static final InterProcessMutex getSystemLock(
            @Nonnull CuratorFramework client)
            throws PersistenceException {
        return getZkLock(client, ZK_ROOT_LOCK);
    }

    /**
     * Get the distributed lock instance for the specified Application Group.
     *
     * @param client - Curator Framework client handle.
     * @param group  - Application Group to Lock.
     * @return - Lock instance.
     * @throws PersistenceException
     */
    public static final InterProcessMutex getZkLock(
            @Nonnull CuratorFramework client,
            @Nonnull ApplicationGroup group)
            throws PersistenceException {
        String path = group.getAbsolutePath();
        return getZkLock(client, path);
    }

    /**
     * Get the distributed lock instance for the specified Application.
     *
     * @param client      - Curator Framework client handle.
     * @param application - Application to Lock.
     * @return - Lock instance.
     * @throws PersistenceException
     */
    public static final InterProcessMutex getZkLock(
            @Nonnull CuratorFramework client,
            @Nonnull Application application)
            throws PersistenceException {
        String path = application.getAbsolutePath();
        return getZkLock(client, path);
    }

    /**
     * Get the distributed lock instance for the specified Application.
     *
     * @param client        - Curator Framework client handle.
     * @param configuration - Configuration node to Lock.
     * @param version       - Configuration version to lock for.
     * @return - Lock instance.
     * @throws PersistenceException
     */
    public static final InterProcessMutex getZkLock(
            @Nonnull CuratorFramework client,
            @Nonnull PersistedConfigNode configuration,
            @Nonnull Version version)
            throws PersistenceException {
        String path = String.format("%s/%d", configuration.getAbsolutePath(),
                version.getMajorVersion());
        return getZkLock(client, path);
    }

    /**
     * Get the ZooKeeper path for the specified node.
     *
     * @param node - Node to coreEnv path for.
     * @return - ZooKeeper path.
     * @throws PersistenceException
     */
    public static final String getZkPath(@Nonnull IZkNode node)
            throws PersistenceException {
        return String.format("%s/%s", getServerRootPath(), node.getAbsolutePath());
    }

    /**
     * Get the ZooKeeper path of the specified path String
     * relative to the configuration.
     *
     * @param configNode - Root configuration node.
     * @param path       - Path to append.
     * @return - Absolute Path
     * @throws PersistenceException
     */
    public static final String getZkPath(@Nonnull PersistedConfigNode configNode,
                                         @Nonnull String path)
            throws PersistenceException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        String p = getZkPath(configNode);
        StringBuilder buff = new StringBuilder(p);
        String[] parts = path.split("\\.");
        if (parts.length > 0) {
            for (String part : parts) {
                buff.append("/").append(part);
            }
        }
        return buff.toString();
    }

    /**
     * Get the ZooKeeper path for the specified configuration.
     *
     * @param configuration - Configuration instance.
     * @return - ZooKeeper Path.
     * @throws PersistenceException
     */
    public static final String getZkPath(@Nonnull Configuration configuration)
            throws PersistenceException {
        String path = String.format("%s/%s/%s", configuration.getApplicationGroup(),
                configuration.getApplication(),
                configuration.getName());
        return String.format("%s/%s/%d", getServerRootPath(), path,
                configuration.getVersion().getMajorVersion());
    }

    /**
     * Get the ZooKeeper path for the specified configuration.
     *
     * @param group - Application Group instance.
     * @return - ZooKeeper Path.
     * @throws PersistenceException
     */
    public static final String getZkPath(@Nonnull ApplicationGroup group)
            throws PersistenceException {
        return String.format("%s%s", getServerRootPath(), group.getAbsolutePath());
    }

    /**
     * Get the ZooKeeper path for the specified configuration.
     *
     * @param application - Application instance.
     * @return - ZooKeeper Path.
     * @throws PersistenceException
     */
    public static final String getZkPath(@Nonnull Application application)
            throws PersistenceException {
        return String
                .format("%s%s", getServerRootPath(), application.getAbsolutePath());
    }

    /**
     * Get the ZooKeeper path for the specified application group.
     *
     * @param group - Application Group name.
     * @return - ZooKeeper Path.
     * @throws PersistenceException
     */
    public static final String getZkPath(@Nonnull String group) throws
            PersistenceException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        return String.format("%s/%s", getServerRootPath(), group);
    }

    /**
     * Get the ZooKeeper path for the specified application group.
     *
     * @param group       - Application Group instance.
     * @param application - Application name.
     * @return - ZooKeeper Path.
     * @throws PersistenceException
     */
    public static final String getZkPath(ApplicationGroup group,
                                         @Nonnull String application)
            throws PersistenceException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(application));
        return String
                .format("%s%s/%s", getServerRootPath(), group.getAbsolutePath(),
                        application);
    }

    /**
     * Get the ZooKeeper path for the specified configname application.
     *
     * @param application - Application instance.
     * @param configname  - Configuration name.
     * @return - ZooKeeper Path.
     * @throws PersistenceException
     */
    public static final String getZkPath(@Nonnull Application application,
                                         @Nonnull String configname,
                                         @Nonnull Version version)
            throws PersistenceException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configname));
        return String
                .format("%s%s/%s/%d", getServerRootPath(),
                        application.getAbsolutePath(),
                        configname, version.getMajorVersion());
    }

    /**
     * Extract the node name from the path.
     *
     * @param path - ZooKeeper Path.
     * @return - Extracted Node name.
     */
    public static final String getNodeNameFromPath(String path) {
        if (!Strings.isNullOrEmpty(path)) {
            String[] parts = path.split("/");
            if (parts.length > 0) {
                return parts[parts.length - 1];
            }
        }
        return null;
    }
}
