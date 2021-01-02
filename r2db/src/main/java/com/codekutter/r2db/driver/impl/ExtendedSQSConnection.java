/*
 *  Copyright (2021) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
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

package com.codekutter.r2db.driver.impl;

import com.amazon.sqs.javamessaging.*;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.codekutter.common.messaging.AbstractJmsConnection;
import com.codekutter.common.messaging.AwsSQSConnection;
import com.codekutter.common.stores.ConnectionException;
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigParametersNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;

import javax.jms.Session;
import java.io.IOException;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class ExtendedSQSConnection extends AbstractJmsConnection {
    public static final String DEFAULT_PROFILE = "default";
    @ConfigAttribute(required = true)
    private String region;
    @ConfigAttribute
    private String profile = DEFAULT_PROFILE;
    @ConfigAttribute(required = true)
    private String s3bucket;
    @Setter(AccessLevel.NONE)
    private AmazonSQS client;
    @Setter(AccessLevel.NONE)
    private AwsS3Connection s3Connection;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private SQSConnectionFactory connectionFactory = null;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private SQSConnection connection;


    @Override
    public Session connection() throws ConnectionException {
        try {
            state().checkOpened();
            if (autoAck())
                return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            else
                return connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        } catch (Throwable t) {
            throw new ConnectionException(t, getClass());
        }
    }

    @Override
    public boolean hasTransactionSupport() {
        return false;
    }

    @Override
    public void close(@NotNull Session connection) throws ConnectionException {

    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@NotNull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (!(cnode instanceof ConfigPathNode)) {
                throw new ConfigurationException(String.format("Invalid connection configuration. [node=%s]", node.getAbsolutePath()));
            }
            s3Connection = new AwsS3Connection();
            s3Connection.configure(node);

            ExtendedClientConfiguration config = configBuilder((ConfigPathNode) cnode);
            ProfileCredentialsProvider provider = new ProfileCredentialsProvider(profile);
            // Only to check a valid profile is specified.
            provider.getCredentials();
            client = new AmazonSQSExtendedClient(AmazonSQSClientBuilder
                    .standard()
                    .withRegion(region)
                    .withCredentials(provider).build(),
                    config);
            connectionFactory = new SQSConnectionFactory(
                    new ProviderConfiguration(),
                    client
            );
            connection = connectionFactory.createConnection();
            connection.start();
            state().setState(EConnectionState.Open);
        } catch (Throwable t) {
            state().setError(t);
            throw new ConfigurationException(t);
        }

    }

    private ExtendedClientConfiguration configBuilder(ConfigPathNode node) throws ConfigurationException, ConnectionException {
        ExtendedClientConfiguration config = new ExtendedClientConfiguration();
        config.withPayloadSupportEnabled(s3Connection.connection(), s3bucket).withAlwaysThroughS3(false);
        ConfigParametersNode params = node.parmeters();
        if (params != null && !params.getKeyValues().isEmpty()) {
            Map<String, ConfigValueNode> values = params.getKeyValues();
            for (String f : values.keySet()) {
                boolean ret = ReflectionUtils.setValueFromString(values.get(f).getValue(), config, ClientConfiguration.class, f);
                if (!ret) {
                    LogUtils.warn(AwsSQSConnection.class, String.format("Ignored Invalid configuration : [property=%s]", f));
                } else {
                    LogUtils.debug(AwsSQSConnection.class, String.format("Set client configuration [property=%s]", f));
                }
            }
        }
        return config;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (state().isOpen()) {
            state().setState(EConnectionState.Closed);
        }
        client = null;
    }
}
