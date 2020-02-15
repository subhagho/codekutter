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

import com.amazon.sqs.javamessaging.SQSConnection;
import com.codekutter.common.model.IKeyed;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.KeyValuePair;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.Monitoring;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.jms.*;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public abstract class AbstractSQSDbCachedQueue<M extends IKeyed> extends DbCachedQueue<SQSConnection, M> {
    @ConfigAttribute(required = true)
    private String queue;
    @ConfigValue
    private boolean autoAck = false;
    @Setter(AccessLevel.NONE)
    private AwsSQSConnection connection;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.MODULE)
    private Session session;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.MODULE)
    private MessageProducer producer;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.MODULE)
    private MessageConsumer consumer;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.MODULE)
    private Map<String, Message> messageCache = new ConcurrentHashMap<>();



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
            queue = URLDecoder.decode(queue, StandardCharsets.UTF_8.name());
            LogUtils.info(getClass(), String.format("Configuring Queue. [name=%s]...", name()));
            AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (!(cnode instanceof ConfigPathNode)) {
                throw new ConfigurationException(String.format("Invalid Queue configuration. [node=%s]", node.getAbsolutePath()));
            }
            AbstractConnection<SQSConnection> conn = ConnectionManager.get().readConnection((ConfigPathNode) cnode);
            if (!(conn instanceof AwsSQSConnection)) {
                throw new ConfigurationException(String.format("Invalid SQS connection returned. [type=%s]", conn.getClass().getCanonicalName()));
            }
            connection = (AwsSQSConnection) conn;
            if (autoAck)
                session = connection.connection().createSession(false, Session.AUTO_ACKNOWLEDGE);
            else
                session = connection.connection().createSession(false, Session.CLIENT_ACKNOWLEDGE);

            setupMetrics(queue);
        } catch (Throwable t) {
            throw new ConfigurationException(t);
        }
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                if (producer != null) {
                    producer.close();
                }
                if (consumer != null) {
                    consumer.close();
                }
                session.close();
                connection.close();
            } catch (JMSException ex) {
                throw new IOException(ex);
            }
        }
    }

    @Override
    public void send(@Nonnull M message, @Nonnull Principal user) throws JMSException {
        try {
            sendLatency.record(() -> {
                try {
                    if (producer == null) {
                        producer = session.createProducer(session.createQueue(queue));
                    }
                    TextMessage m = message(message);
                    producer.send(m);

                    Monitoring.increment(sendCounter.name(), (KeyValuePair<String, String>[]) null);
                } catch (Exception ex) {
                    LogUtils.error(getClass(), ex);
                    Monitoring.increment(sendErrorCounter.name(), (KeyValuePair<String, String>[]) null);
                    throw new RuntimeException(ex);
                }
            });
        } catch (Exception ex) {
            Monitoring.increment(sendErrorCounter.name(), (KeyValuePair<String, String>[]) null);
            LogUtils.error(getClass(), ex);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }

    @Override
    public M receive(long timeout, @Nonnull Principal user) throws JMSException {
        try {
            return receiveLatency.record(() -> receiveMessage(timeout, user));
        } catch (Exception ex) {
            Monitoring.increment(receiveErrorCounter.name(), (KeyValuePair<String, String>[]) null);
            LogUtils.error(getClass(), ex);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }

    private M receiveMessage(long timeout, Principal user) throws JMSException {
        try {
            if (consumer == null) {
                consumer = session.createConsumer(session.createQueue(queue));
            }
            Message m = consumer.receive(timeout);
            if (m != null) {
                Monitoring.increment(receiveCounter.name(), (KeyValuePair<String, String>[]) null);
                if (m instanceof TextMessage) {
                    if (!autoAck) {
                        messageCache.put(m.getJMSMessageID(), m);
                    }
                    return message((TextMessage) m, user);
                } else {
                    throw new JMSException(String.format("Invalid message type. [type=%s]", m.getClass().getCanonicalName()));
                }
            }
            return null;
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            Monitoring.increment(receiveErrorCounter.name(), (KeyValuePair<String, String>[]) null);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }

    @Override
    public boolean ack(@Nonnull String messageId, @Nonnull Principal user) throws JMSException {
        if (!autoAck) {
            if (messageCache.containsKey(messageId)) {
                Message message = messageCache.remove(messageId);
                message.acknowledge();
                return true;
            }
        }
        return false;
    }

    @Override
    public List<M> receiveBatch(int maxResults, long timeout, @Nonnull Principal user) throws JMSException {
        long stime = System.currentTimeMillis();
        long tleft = timeout;
        List<M> messages = new ArrayList<>();
        while (tleft > 0 && messages.size() < maxResults) {
            M m = receive(timeout, user);
            if (m != null) {
                messages.add(m);
            }
            tleft = (timeout - (System.currentTimeMillis() - stime));
        }
        if (!messages.isEmpty()) {
            return messages;
        }
        return null;
    }

    public abstract TextMessage message(M message) throws JMSException;

    public abstract M message(TextMessage message, Principal user) throws JMSException;
}
