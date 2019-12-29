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
 * Date: 16/2/19 12:13 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core.zookeeper;

import com.codekutter.zconfig.common.model.annotations.ConfigParam;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;

/**
 * Connection configuration settings for the ZooKeeper clients.
 */
@ConfigPath(path = "zconfig/server/zookeeper/settings")
public class ZkConnectionConfig {
    /**
     * ZooKeeper connection String.
     */
    @ConfigValue(name = "connectionString", required = true)
    private String connectionString;
    /**
     * Retry type - Implementation class.
     */
    @ConfigParam(name = "retry@type")
    private String retryClass;
    /**
     * Retry sleep time.
     */
    @ConfigParam(name = "retry@sleepTime")
    private int sleepTime;
    /**
     * Retry max attempts.
     */
    @ConfigParam(name = "retry@retries")
    private int maxRetries;

    /**
     * Root path for this server in ZooKeeper.
     */
    @ConfigValue(name = "rootPath", required = true)
    private String rootPath;

    /**
     * Get the ZooKeeper connection String.
     *
     * @return - ZooKeeper connection String.
     */
    public String getConnectionString() {
        return connectionString;
    }

    /**
     * Set the ZooKeeper connection String.
     *
     * @param connectionString - ZooKeeper connection String.
     */
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    /**
     * Get the root path for this server.
     *
     * @return - Server root path.
     */
    public String getRootPath() {
        return rootPath;
    }

    /**
     * Set the root path for this server.
     *
     * @param rootPath - Server root path.
     */
    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    /**
     * Get the Retry implementation class.
     *
     * @return - Retry implementation class.
     */
    public String getRetryClass() {
        return retryClass;
    }

    /**
     * Set the Retry implementation class.
     *
     * @param retryClass - Retry implementation class.
     */
    public void setRetryClass(String retryClass) {
        this.retryClass = retryClass;
    }

    /**
     * Get the Retry sleep time.
     *
     * @return - Retry sleep time
     */
    public int getSleepTime() {
        return sleepTime;
    }

    /**
     * Set the Retry sleep time.
     *
     * @param sleepTime - Retry sleep time
     */
    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }

    /**
     * Get the Retry max attempts.
     *
     * @return - Retry max attempts
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Set the Retry max attempts.
     *
     * @param maxRetries - Retry max attempts
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
}
