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
import com.codekutter.common.utils.Monitoring;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Timer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.jms.JMSException;
import java.io.Closeable;
import java.security.Principal;
import java.util.List;

/**
 * Wrapper class for creating JMS based messaging instances.
 * Only support Queue interfaces.
 *
 * @param <C> - Queue Connection type
 * @param <M> - Message Type
 */
@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "queue")
public abstract class AbstractQueue<C, M> implements IConfigurable, Closeable {
    private static final class Metrics {
        private static final String METRIC_LATENCY_SEND = String.format("%s.%s.%s.SEND", "%s", "%s", "%s");
        private static final String METRIC_LATENCY_RECEIVE = String.format("%s.%s.%s.RECEIVE", "%s", "%s", "%s");
        private static final String METRIC_COUNTER_SEND_ERROR = String.format("%s.%s.%s.ERRORS.SEND", "%s", "%s", "%s");
        private static final String METRIC_COUNTER_RECV = String.format("%s.%s.%s.COUNT.RECEIVE", "%s", "%s", "%s");
        private static final String METRIC_COUNTER_SEND = String.format("%s.%s.%s.COUNT.SEND", "%s", "%s", "%s");
        private static final String METRIC_COUNTER_RECV_ERROR = String.format("%s.%s.%s.ERRORS.RECEIVE", "%s", "%s", "%s");
    }
    /**
     * Default Queue receive message timeout.
     */
    private static final long DEFAULT_RECEIVE_TIMEOUT = 30 * 1000;

    /**
     * Message queue name.
     */
    @ConfigAttribute(required = true)
    private String name;
    /**
     * Queue receive message timeout.
     */
    @ConfigValue(name = "receiveTimeout")
    private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

    @ConfigAttribute
    private boolean audited = false;

    @ConfigValue
    private String auditLogger;

    /**
     * Queue connection handle.
     */
    @Setter(AccessLevel.NONE)
    private AbstractConnection<C> connection;

    /**
     * Metrics - Send Latency
     */
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    protected Timer sendLatency = null;
    /**
     * Metrics - Receive Latency
     */
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    protected Timer receiveLatency = null;
    /**
     * Counter - Send events
     */
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    protected Id sendCounter = null;
    /**
     * Counter - Receive events.
     */
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    protected Id receiveCounter = null;
    /**
     * Counter - Receive Error events.
     */
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    protected Id receiveErrorCounter = null;
    /**
     * Counter - Send Error events.
     */
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    protected Id sendErrorCounter = null;

    /**
     * Send a message to the queue.
     *
     * @param message - Message handle.
     * @throws JMSException
     */
    public abstract void send(@Nonnull M message, @Nonnull Principal user) throws JMSException;

    /**
     * Receive a message from the queue.
     *
     * @param timeout - Message receive timeout.
     * @return - Received message.
     * @throws JMSException
     */
    public abstract M receive(long timeout, @Nonnull Principal user) throws JMSException;

    /**
     * Acknowledge the receipt of the message with the
     * specified JMS message ID.
     *
     * @param messageId - JMS Message ID.
     * @return - ACK successful?
     * @throws JMSException
     */
    public abstract boolean ack(@Nonnull String messageId, @Nonnull Principal user) throws JMSException;

    /**
     * Receive a batch of messages, with the specified batch size and
     * receive timeout.
     * <p>
     * Call will return if batch size is met or timeout occurred.
     *
     * @param maxResults - Batch size of messages to fetch.
     * @param timeout    - Max read timeout.
     * @return - List of read messages.
     * @throws JMSException
     */
    public abstract List<M> receiveBatch(int maxResults, long timeout, @Nonnull Principal user) throws JMSException;


    /**
     * Receive a batch of messages, with the specified batch size and
     * the default receive timeout.
     * <p>
     * Call will return if batch size is met or timeout occurred.
     *
     * @param maxResults - Batch size of messages to fetch.
     * @return - List of read messages.
     * @throws JMSException
     */
    public List<M> receiveBatch(int maxResults, @Nonnull Principal user) throws JMSException {
        return receiveBatch(maxResults, receiveTimeout, user);
    }

    /**
     * Read next message available in the queue. If no message immediately available
     * in the queue the call will wait for the default receive timeout for a
     * message to arrive.
     *
     * @return - Read message or NULL if queue is empty
     * @throws JMSException
     */
    public M receive(@Nonnull Principal user) throws JMSException {
        return receive(receiveTimeout, user);
    }

    protected void setupMetrics(String queue) {
        sendLatency = Monitoring.addTimer(String.format(Metrics.METRIC_LATENCY_SEND, getClass().getCanonicalName(), name(), queue));
        receiveLatency = Monitoring.addTimer(String.format(Metrics.METRIC_LATENCY_RECEIVE, getClass().getCanonicalName(), name(), queue));
        sendCounter = Monitoring.addCounter(String.format(Metrics.METRIC_COUNTER_SEND, getClass().getCanonicalName(), name(), queue));
        receiveCounter = Monitoring.addCounter(String.format(Metrics.METRIC_COUNTER_RECV, getClass().getCanonicalName(), name(), queue));
        receiveErrorCounter = Monitoring.addCounter(String.format(Metrics.METRIC_COUNTER_RECV_ERROR, getClass().getCanonicalName(), name(), queue));
        sendErrorCounter = Monitoring.addCounter(String.format(Metrics.METRIC_COUNTER_SEND_ERROR, getClass().getCanonicalName(), name(), queue));
    }


    public QueueAuditContext context() {
        QueueAuditContext ctx = new QueueAuditContext();
        ctx.setQueueType(getClass().getCanonicalName());
        ctx.setQueueName(name);
        return ctx;
    }
}
