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
 * Date: 4/3/19 9:36 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.rabbitmq;

/**
 * Constants to be used for communicating with the configuration server using RabbitMQ.
 */
public class RMQChannelConstants {
    /**
     * Client registration channel name.
     */
    public static final String RMQ_ADMIN_CHANNEL = "ZCONFIG_ADMIN_CHANNEL";
    /**
     * Client Admin Queue name.
     */
    public static final String RMQ_REGISTER_QUEUE = "ZCONFIG_ADMIN_CLIENT";
    /**
     * Client Updates Exchange name.
     */
    public static final String RMQ_UPDATE_CHANNEL = "ZCONFIG_UPDATES_CHANNEL";
    /**
     * Routing Key to be used for registration.
     */
    public static final String RMQ_REGISTER_ROUTING_KEY = "REGISTER";
    /**
     * Routing Key to be used for shutdown.
     */
    public static final String RMQ_SHUTDOWN_ROUTING_KEY = "SHUTDOWN";

    private static final String RMQ_UPDATE_QUEUE_PREFIX = "ZCONFIG_UPDATES_%s";

    /**
     * Get the channel name to listen for configuration updates for the group.
     *
     * @param group - Application Group name.
     * @return - Update Channel name.
     */
    public static final String getGroupUpdateQueue(String group) {
        return String.format(RMQ_UPDATE_QUEUE_PREFIX, group);
    }
}
