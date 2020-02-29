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

import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionException;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import io.confluent.kafka.jms.KafkaConnectionFactory;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.Session;
import java.io.IOException;

@Getter
@Setter
@Accessors(fluent = true)
public class KafkaConnection extends AbstractConnection<Session> {
    @ConfigAttribute(name = "client", required = true)
    private String clientId;
    @ConfigValue(name = "servers", required = true)
    private String servers;
    @ConfigValue(name = "zookeeper", required = true)
    private String zookeeperHost;
    @ConfigAttribute(name = "auto-ack")
    private boolean autoAck = false;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private KafkaConnectionFactory connectionFactory;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Connection connection;

    @Override
    public Session connection() throws ConnectionException {
        return null;
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

    }

    @Override
    public void close() throws IOException {

    }
}
