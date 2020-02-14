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
import com.codekutter.common.model.DbMessage;
import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.IKeyed;
import com.codekutter.common.model.ObjectState;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public abstract class DbCachedQueue<C, M extends IKeyed> extends AbstractQueue<C, M> {
    public static final int DEFAULT_THREAD_POOL_SIZE = 8;
    public static final int DEFAULT_FETCH_BATCH_SIZE = 32;

    @ConfigValue(name = "dbConnection")
    private String dbConnectionName;
    @ConfigValue
    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    @ConfigValue
    private int fetchBatchSize = DEFAULT_FETCH_BATCH_SIZE;
    @Setter(AccessLevel.NONE)
    private HibernateConnection dbConnection;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ExecutorService executorService;
    private ReentrantLock __lock = new ReentrantLock();

    @Setter(AccessLevel.NONE)
    private ObjectState state = new ObjectState();

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

    @SuppressWarnings("unchecked")
    public List<M> sendNextBatch(@Nonnull String instanceId,
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

                List<M> resutls = new ArrayList<>();
                List<DbMessage> messages = query.getResultList();
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
                            } else {
                                dbm.setInstanceId(instanceId);
                                resutls.add(m);
                            }
                        } catch (Exception ex) {
                            dbm.setState(ESendState.Error);
                            dbm.setError(ex.getLocalizedMessage());
                            LogUtils.error(getClass(), ex);
                        }
                        session.save(dbm);
                    }
                }
                tx.commit();
                if (!resutls.isEmpty()) {
                    for (M message : resutls) {

                    }
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

    @SuppressWarnings("unchecked")
    public void updateProcessed(@Nonnull String instanceId,
                                @Nonnull Session session,
                                @Nonnull List<M> records,
                                Map<String, String> errors,
                                Class<? extends M> type) throws JMSException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId));
        Preconditions.checkArgument(!records.isEmpty());

        Map<String, M> rMap = new HashMap<>();
        for (M r : records) {
            rMap.put(r.getKey().stringKey(), r);
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

            executorService = Executors.newFixedThreadPool(threadPoolSize);

            state.setState(EObjectState.Available);
        } catch (Exception ex) {
            state.setError(ex);
            throw ex;
        }
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }

    public abstract byte[] getBytes(@Nonnull M message) throws JMSException;

    public abstract M readMessage(@Nonnull byte[] body) throws JMSException;
}
