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
 * Date: 10/2/19 6:32 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.rabbitmq;

import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.*;
import com.codekutter.zconfig.common.model.annotations.ConfigParam;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.MethodInvoke;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;

/**
 * RabbitMQ Connection Factory - Class abstracts the RabbitMQ Connection.
 */
@ConfigPath(path = "rmq/settings")
public class RMQConnectionFactory implements IConfigurable, Closeable {
    /**
     * Default Virtual Host value.
     */
    private static final String DEFAULT_VIRTUAL_HOST = "/";
    /**
     * Default RabbitMQ port. (TLS Port)
     */
    private static final int DEFAULT_PORT = 5671;

    /**
     * State instance of this connection factory.
     */
    private ClientState state = new ClientState();
    /**
     * Virtual Host for RabbitMQ
     */
    @ConfigParam(name = "virtualHost")
    private String virtualHost;
    /**
     * Hostname of the RabbitMQ Server
     */
    @ConfigParam(name = "hostname")
    private String hostname;
    /**
     * Port the server is running on.
     */
    @ConfigParam(name = "port")
    private int port = -1;

    /**
     * RabbitMQ Connection factory.
     */
    private ConnectionFactory connectionFactory;

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    @MethodInvoke
    public void configure(@Nonnull AbstractConfigNode node)
    throws ConfigurationException {
        Preconditions.checkArgument(node != null);
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        try {
            ConfigurationAnnotationProcessor
                    .readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            setup();

            state.setState(EClientState.Initialized);
        } catch (Exception e) {
            state.setError(e);
            throw new ConfigurationException(e);
        }
    }

    /**
     * Setup this factory instance.
     *
     * @throws ConfigurationException
     */
    private void setup() throws ConfigurationException {
        if (Strings.isNullOrEmpty(virtualHost)) {
            virtualHost = DEFAULT_VIRTUAL_HOST;
        }
        if (Strings.isNullOrEmpty(hostname)) {
            throw new ConfigurationException(
                    String.format("Missing configuration parameter : [%s]",
                                  "hostname"));
        }
        if (port <= 0) {
            port = DEFAULT_PORT;
        }
    }

    /**
     * Open this connection factory instance.
     * Method will open a test connection to the server.
     *
     * @param username - Username to connect with.
     * @param password - Password to connect with.
     * @throws RMQException
     */
    public void open(@Nonnull String username, @Nonnull String password)
    throws RMQException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(username));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        try {
            state.checkState(EClientState.Initialized);
            connectionFactory = new ConnectionFactory();
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);
            connectionFactory.setVirtualHost(virtualHost);
            connectionFactory.setHost(hostname);
            connectionFactory.setPort(port);
            try (Connection connection = connectionFactory.newConnection()) {

                LogUtils.info(getClass(),
                              "RabbitMQ Connection successfully initialized...");
            }
            state.setState(EClientState.Available);
        } catch (Exception e) {
            state.setError(e);
            throw new RMQException(e);
        }
    }

    /**
     * Get a new connection instance from this factory.
     *
     * @return - New connection instance.
     * @throws RMQException
     */
    public Connection getConnection() throws RMQException {
        try {
            state.checkState(EClientState.Available);
            return connectionFactory.newConnection();
        } catch (Exception e) {
            throw new RMQException(e);
        }
    }

    /**
     * Close this connection factory.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        state.dispose();
        if (connectionFactory != null) {
            connectionFactory = null;
        }
    }

    /**
     * Get the Virtual Host for this connection factory.
     *
     * @return - RabbitMQ Virtual Host
     */
    public String getVirtualHost() {
        return virtualHost;
    }

    /**
     * Set the Virtual Host for this connection factory.
     *
     * @param virtualHost - RabbitMQ Virtual Host
     */
    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    /**
     * Get the RabbitMQ Server hostname.
     *
     * @return - RabbitMQ Server hostname
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * Set the RabbitMQ Server hostname.
     *
     * @param hostname - RabbitMQ Server hostname
     */
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    /**
     * Get the RabbitMQ Server port.
     *
     * @return - RabbitMQ Server port
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the RabbitMQ Server port.
     *
     * @param port - RabbitMQ Server port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Get the state of this Connection factory.
     *
     * @return - Connection factory state.
     */
    public EClientState getState() {
        return state.getState();
    }
}
