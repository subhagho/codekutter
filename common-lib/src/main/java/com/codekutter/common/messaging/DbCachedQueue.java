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

import com.codekutter.common.GlobalConstants;
import com.codekutter.common.model.*;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.stores.impl.HibernateConnection;
import com.codekutter.common.utils.CypherUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.common.Strings;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.annotation.Nonnull;
import javax.jms.JMSException;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import java.io.IOException;
import java.security.Principal;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public abstract class DbCachedQueue<C, M extends IKeyed> extends CachedQueue<C, M> {

    @ConfigValue(name = "dbConnection")
    private String dbConnectionName;
    @Setter(AccessLevel.NONE)
    private HibernateConnection dbConnection;

    private ReentrantLock __lock = new ReentrantLock();

    public HibernateConnection getDbConnection() throws Exception {
        __lock.lock();
        try {
            if (dbConnection == null) {
                dbConnection = (HibernateConnection) ConnectionManager.get().connection(dbConnectionName, Session.class);
                if (dbConnection == null) {
                    throw new Exception(String.format("Error getting DB connection. [name=%s][type=%s]", dbConnectionName, HibernateConnection.class));
                }
            }
            return dbConnection;
        } finally {
            __lock.unlock();
        }
    }

    public void send(@Nonnull M message,
                     @Nonnull Session session,
                     @Nonnull Principal user) throws JMSException {
        try {
            byte[] data = getBytes(message);
            if (data == null || data.length <= 0) {
                throw new JMSException("Error serializing message : NULL/Empty buffer returned.");
            }
            String checksum = CypherUtils.getHash(data);
            DbMessage dbm = new DbMessage();
            dbm.setMessageId(UUID.randomUUID().toString());
            dbm.setQueue(name());
            dbm.setPartition((int) (Thread.currentThread().getId() % threadPoolSize));
            dbm.setBody(data);
            dbm.setMessageType(message.getClass().getCanonicalName());
            dbm.setChecksum(checksum);
            dbm.setLength(data.length);
            dbm.setState(ESendState.New);
            dbm.setCreatedTimestamp(System.currentTimeMillis());
            String userJson = GlobalConstants.getJsonMapper().writeValueAsString(user);
            dbm.setSender(userJson);

            session.save(dbm);
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }

    @Override
    public List<MessageStruct<M>> sendNextBatch(@Nonnull String instanceId,
                                                int partition,
                                                @Nonnull Class<? extends M> type) throws JMSException {
        Preconditions.checkArgument(partition >= 0 && partition < threadPoolSize);
        try {
            try (Session session = getDbConnection().connection()) {
                return sendNextBatch(instanceId, partition, session, type);
            }
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private List<MessageStruct<M>> sendNextBatch(@Nonnull String instanceId,
                                                 int partition,
                                                 @Nonnull Session session,
                                                 @Nonnull Class<? extends M> type) throws JMSException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId));
        __lock.lock();
        try {
            Transaction tx = session.beginTransaction();
            try {
                state.check(EObjectState.Available, getClass());
                String qstr = String.format("FROM %s WHERE queue = :queue AND partition = :partition " +
                                "AND (state = :state_n OR state = :state_e) AND instanceId is null ORDER BY createdTimestamp",
                        DbMessage.class.getCanonicalName());
                Query query = session.createQuery(qstr).setLockMode(LockModeType.PESSIMISTIC_WRITE).setMaxResults(fetchBatchSize);
                query.setParameter("queue", name());
                query.setParameter("partition", partition);
                query.setParameter("state_n", ESendState.New.name());
                query.setParameter("state_e", ESendState.Error.name());

                List<MessageStruct<M>> resutls = new ArrayList<>();
                List<DbMessage> messages = query.getResultList();
                Map<String, String> errors = new HashMap<>();

                if (messages != null && !messages.isEmpty()) {
                    for (DbMessage dbm : messages) {
                        byte[] body = dbm.getBody();
                        try {
                            M m = readMessage(body);
                            if (m == null) {
                                dbm.setState(ESendState.Error);
                                String err = String.format("Error reading entity from record. [type=%s][id=%s]", type.getCanonicalName(), dbm.getMessageId());
                                dbm.setError(err);
                                LogUtils.error(getClass(), err);
                                errors.put(dbm.getMessageId(), err);
                                dbm.setRetryCount(dbm.getRetryCount() + 1);
                            } else {
                                dbm.setInstanceId(instanceId);
                                MessageStruct<M> ms = new MessageStruct<>();
                                ms.message(m);
                                ms.user(GlobalConstants.getJsonMapper().readValue(dbm.getSender(), Principal.class));
                                resutls.add(ms);
                            }
                        } catch (Exception ex) {
                            dbm.setState(ESendState.Error);
                            dbm.setError(ex.getLocalizedMessage());
                            LogUtils.error(getClass(), ex);
                            errors.put(dbm.getMessageId(), ex.getLocalizedMessage());
                            dbm.setRetryCount(dbm.getRetryCount() + 1);
                            dbm.setEx(ex);
                        }
                        boolean saved = false;
                        if (dbm.getState() == ESendState.Error) {
                            if (dbm.getRetryCount() > retryCount) {
                                String err = dbm.getError();
                                if (dbm.getEx() != null) {
                                    err = LogUtils.getStackTrace(dbm.getEx());
                                }
                                sendError(session, dbm, type, err);
                                session.delete(dbm);
                                saved = true;
                            }
                        }
                        if (!saved)
                            session.save(dbm);
                    }
                }
                tx.commit();
                if (!resutls.isEmpty()) {
                    for (MessageStruct<M> message : resutls) {
                        send(message.message(), message.user());
                    }
                    updateProcessed(instanceId, session, resutls, errors, type);
                    return resutls;
                }
                return null;
            } catch (Exception ex) {
                tx.rollback();
                throw ex;
            }
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            throw new JMSException(t.getLocalizedMessage());
        } finally {
            __lock.unlock();
        }
    }

    private void sendError(Session session, DbMessage message, Class<? extends M> type, String err) throws Exception {
        DbMessageError error = new DbMessageError();
        error.setBody(message.getBody());
        error.setChecksum(message.getChecksum());
        error.setCreatedTimestamp(message.getCreatedTimestamp());
        error.setError(err);
        error.setLength(message.getLength());
        error.setMessageId(message.getMessageId());
        error.setMessageType(type.getCanonicalName());
        error.setQueue(message.getQueue());
        error.setSender(message.getSender());
        error.setSentTimestamp(message.getSentTimestamp());
        error.setState(ESendState.Error);

        session.save(error);
    }

    @SuppressWarnings("unchecked")
    private void updateProcessed(@Nonnull String instanceId,
                                 @Nonnull Session session,
                                 @Nonnull List<MessageStruct<M>> records,
                                 Map<String, String> errors,
                                 Class<? extends M> type) throws JMSException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId));
        Preconditions.checkArgument(!records.isEmpty());

        Map<String, M> rMap = new HashMap<>();
        for (MessageStruct<M> r : records) {
            rMap.put(r.message().getKey().stringKey(), r.message());
        }
        __lock.lock();
        try {
            state.check(EObjectState.Available, getClass());
            Transaction tx = session.beginTransaction();
            try {
                String qstr = String.format("FROM %s WHERE (state = :state_n OR state = :state_e) AND instanceId = :instance_id", DbMessage.class.getCanonicalName());
                Query query = session.createQuery(qstr).setLockMode(LockModeType.PESSIMISTIC_WRITE).setMaxResults(fetchBatchSize);
                query.setParameter("state_n", ESendState.New);
                query.setParameter("state_e", ESendState.Error);
                query.setParameter("instance_id", instanceId);

                List<DbMessage> messages = query.getResultList();
                if (messages != null && !messages.isEmpty()) {
                    for (DbMessage dbm : messages) {
                        byte[] body = dbm.getBody();
                        M m = readMessage(body);
                        if (m == null) {
                            String err = String.format("Error reading entity from record. [type=%s][id=%s]", type.getCanonicalName(), dbm.getMessageId());
                            throw new JMSException(err);
                        } else {
                            if (rMap.containsKey(m.getKey().stringKey())) {
                                dbm.setInstanceId(null);
                                dbm.setState(ESendState.Sent);
                                if (errors != null && errors.containsKey(m.getKey().stringKey())) {
                                    dbm.setState(ESendState.Error);
                                    String err = errors.get(m.getKey().stringKey());
                                    dbm.setError(err);
                                }
                                session.save(dbm);
                            }
                        }
                    }
                }
                tx.commit();
            } catch (Exception ex) {
                tx.rollback();
                throw ex;
            }
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            throw new JMSException(t.getLocalizedMessage());
        } finally {
            __lock.unlock();
        }
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        try {
            Preconditions.checkArgument(node instanceof ConfigPathNode);
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);

            start();

            state.setState(EObjectState.Available);
        } catch (Exception ex) {
            state.setError(ex);
            throw ex;
        }
    }

    @Override
    public void close() throws IOException {
        if (dbConnection != null) {
            dbConnection.close();
            dbConnection = null;
        }
    }

    public abstract byte[] getBytes(@Nonnull M message) throws JMSException;

    public abstract M readMessage(@Nonnull byte[] body) throws JMSException;
}
