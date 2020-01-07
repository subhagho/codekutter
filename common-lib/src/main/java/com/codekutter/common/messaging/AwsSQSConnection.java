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

package com.codekutter.common.messaging;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.IOException;

@Getter
@Setter
@Accessors(fluent = true)
public class AwsSQSConnection extends AbstractConnection<SQSConnection> {
    @ConfigAttribute(required = true)
    private String region;
    @Setter(AccessLevel.NONE)
    private SQSConnection connection;

    @Override
    public SQSConnection connection() {
        return connection;
    }

    @Override
    public boolean hasTransactionSupport() {
        return false;
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                    new ProviderConfiguration(),
                    AmazonSQSClientBuilder.standard()
                            .withRegion(region)
            );
            connection = connectionFactory.createConnection();
            connection.start();

            state().setState(EConnectionState.Open);
        } catch (Throwable t) {
            state().setError(t);
            throw new ConfigurationException(t);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
