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

import com.codekutter.common.auditing.AuditManager;
import com.codekutter.common.model.AuditRecord;
import com.codekutter.common.model.EAuditType;
import com.codekutter.common.model.IKeyed;
import com.codekutter.common.utils.KeyValuePair;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.Monitoring;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.common.Strings;

import javax.annotation.Nonnull;
import javax.jms.*;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AbstractDbCachedJmsQueue<M extends IKeyed> extends DbCachedQueue<Session, M> {
    @ConfigAttribute(required = true)
    private String queue;
    @Setter(AccessLevel.MODULE)
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

    public void audit(M message, EAuditType auditType, Principal user) throws JMSException {
        if (audited()) {
            try {
                QueueAuditContext ctx = context();
                String changeContext = ctx.json();
                if (Strings.isNullOrEmpty(auditLogger())) {
                    AuditRecord r = AuditManager.get().audit(getClass(), name(), auditType, message, null, changeContext, user);
                    if (r == null) {
                        throw new JMSException(String.format("Error creating audit record. [data store=%s:%s][entity type=%s]",
                                getClass().getCanonicalName(), name(), message.getClass().getCanonicalName()));
                    }
                } else {
                    AuditRecord r = AuditManager.get().audit(getClass(), name(), auditLogger(), auditType, message, null, changeContext, user);
                    if (r == null) {
                        throw new JMSException(String.format("Error creating audit record. [data store=%s:%s][entity type=%s]",
                                getClass().getCanonicalName(), name(), message.getClass().getCanonicalName()));
                    }
                }
            } catch (Exception ex) {
                LogUtils.error(getClass(), ex);
                throw new JMSException(ex.getLocalizedMessage());
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
                    Message m = message(message);
                    producer.send(m);
                    if (audited()) {
                        audit(message, EAuditType.Create, user);
                    }
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
                    if (!((AbstractJmsConnection) connection()).autoAck()) {
                        messageCache.put(m.getJMSMessageID(), m);
                    }
                    M message = message((TextMessage) m, user);
                    if (audited()) {
                        audit(message, EAuditType.Read, user);
                    }
                    return message;
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
        if (!((AbstractJmsConnection) connection()).autoAck()) {
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

    public abstract Message message(M message) throws JMSException;

    public abstract M message(Message message, Principal user) throws JMSException;

    @Override
    public void close() throws IOException {
        if (session != null) {
            try {
                if (producer != null) {
                    producer.close();
                }
                if (consumer != null) {
                    consumer.close();
                }
                session.close();
            } catch (JMSException ex) {
                throw new IOException(ex);
            }
        }
        if (connection() != null) {
            connection().close();
        }
    }

}
