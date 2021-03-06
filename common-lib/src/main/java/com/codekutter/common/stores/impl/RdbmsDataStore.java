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

package com.codekutter.common.stores.impl;

import com.codekutter.common.Context;
import com.codekutter.common.model.BaseEntity;
import com.codekutter.common.model.EEntityState;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.*;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.annotation.Nonnull;
import javax.persistence.Query;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RdbmsDataStore extends TransactionDataStore<Session, Transaction> {

    private static class TransactionCacheElement {
        private Transaction tx;
        private String key;
        private BaseEntity<?> entity;

        public static String generateKey(BaseEntity<?> entity) {
            return String.format("%s[%s]", entity.getClass(), entity.getKey().stringKey());
        }
    }

    protected Session session;
    protected Session readSession;
    private HibernateConnection readConnection = null;
    private Map<String, TransactionCacheElement> transactionCache = new HashMap<>();

    @Override
    public boolean isInTransaction() throws DataStoreException {
        checkThread();
        return (transaction() != null && transaction().isActive());
    }

    @Override
    public void beingTransaction() throws DataStoreException {
        Preconditions.checkState(session != null);
        checkThread();
        if (transaction() == null) {
            if (session.isJoinedToTransaction()) {
                throw new DataStoreException("Session already has a running transaction.");
            }
            transaction(session.beginTransaction());
        } else if (!session.isJoinedToTransaction()) {
            throw new DataStoreException("Transaction handle is set but session has no active transaction.");
        }
    }

    public void flush() throws DataStoreException {
        Preconditions.checkState(session != null);
        checkThread();
        session.flush();
    }

    @Override
    public void commit() throws DataStoreException {
        Preconditions.checkState(session != null);
        Preconditions.checkState(isInTransaction());
        checkThread();

        transactionCache.clear();

        transaction().commit();
        transaction(null);
    }

    @Override
    public void rollback() throws DataStoreException {
        Preconditions.checkState(session != null);
        checkThread();

        if (session.isJoinedToTransaction() && transaction().isActive())
            transaction().rollback();
        transactionCache.clear();

        transaction(null);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <E extends IEntity> E createEntity(@Nonnull E entity,
                                              @Nonnull Class<? extends E> type,
                                              Context context) throws
            DataStoreException {
        Preconditions.checkState(session != null);
        Preconditions.checkState(isInTransaction());
        checkThread();
        if (entity instanceof BaseEntity) {
            if (((BaseEntity) entity).getState().getState() != EEntityState.New) {
                String key = TransactionCacheElement.generateKey((BaseEntity<?>) entity);
                TransactionCacheElement ce = transactionCache.get(key);
                if (ce != null) {
                    Transaction tx = transaction();
                    if (!tx.equals(ce.tx)) {
                        throw new DataStoreException(
                                String.format("Invalid transaction cache: transaction handle is stale. [entity=%s]",
                                        entity.getKey().stringKey()));
                    }
                    if (((BaseEntity<?>) entity).getState().getState() != EEntityState.Synced) {
                        throw new DataStoreException(
                                String.format("Invalid entity state. [entity=%s][state=%s]",
                                        entity.getKey().stringKey(), ((BaseEntity<?>) entity).getState().getState().name()));
                    }
                    return entity;
                } else
                    throw new DataStoreException(
                            String.format("Invalid entity state: [state=%s][id=%s]",
                                    ((BaseEntity) entity).getState().getState().name(), entity.getKey().stringKey()));
            }
        }
        IDGenerator.process(entity, session);
        Object result = session.save(entity);
        if (result == null) {
            throw new DataStoreException(String.format("Error saving entity. [type=%s][key=%s]", type.getCanonicalName(), entity.getKey()));
        }
        if (entity instanceof BaseEntity) {
            ((BaseEntity) entity).getState().setState(EEntityState.Synced);
            TransactionCacheElement ce = new TransactionCacheElement();
            ce.entity = (BaseEntity<?>) entity;
            ce.key = TransactionCacheElement.generateKey((BaseEntity<?>) entity);
            ce.tx = transaction();

            transactionCache.put(ce.key, ce);
        }
        return entity;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <E extends IEntity> E updateEntity(@Nonnull E entity,
                                              @Nonnull Class<? extends E> type,
                                              Context context) throws
            DataStoreException {
        Preconditions.checkState(session != null);
        Preconditions.checkState(isInTransaction());
        checkThread();
        if (entity instanceof BaseEntity) {
            if (((BaseEntity) entity).getState().getState() != EEntityState.Updated) {
                String key = TransactionCacheElement.generateKey((BaseEntity<?>) entity);
                TransactionCacheElement ce = transactionCache.get(key);
                if (ce != null) {
                    Transaction tx = transaction();
                    if (!tx.equals(ce.tx)) {
                        throw new DataStoreException(
                                String.format("Invalid transaction cache: transaction handle is stale. [entity=%s]",
                                        entity.getKey().stringKey()));
                    }
                    if (((BaseEntity<?>) entity).getState().getState() != EEntityState.Synced) {
                        throw new DataStoreException(
                                String.format("Invalid entity state. [entity=%s][state=%s]",
                                        entity.getKey().stringKey(), ((BaseEntity<?>) entity).getState().getState().name()));
                    }
                    return entity;
                } else
                    throw new DataStoreException(
                            String.format("Invalid entity state: [state=%s][id=%s]",
                                    ((BaseEntity) entity).getState().getState().name(), entity.getKey().stringKey()));
            }
        }
        Object result = session.save(entity);
        if (result == null) {
            throw new DataStoreException(String.format("Error updating entity. [type=%s][key=%s]", type.getCanonicalName(), entity.getKey()));
        }
        if (entity instanceof BaseEntity) {
            ((BaseEntity) entity).getState().setState(EEntityState.Synced);
            TransactionCacheElement ce = new TransactionCacheElement();
            ce.entity = (BaseEntity<?>) entity;
            ce.key = TransactionCacheElement.generateKey((BaseEntity<?>) entity);
            ce.tx = transaction();

            transactionCache.put(ce.key, ce);
        }
        return entity;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <E extends IEntity> boolean deleteEntity(@Nonnull Object key,
                                                    @Nonnull Class<? extends E> type,
                                                    Context context) throws
            DataStoreException {
        Preconditions.checkState(session != null);
        Preconditions.checkState(isInTransaction());
        checkThread();

        E entity = findEntity(key, type, context);
        if (entity != null) {
            session.delete(entity);
            if (entity instanceof BaseEntity) {
                ((BaseEntity) entity).getState().setState(EEntityState.Deleted);
                TransactionCacheElement ce = new TransactionCacheElement();
                ce.entity = (BaseEntity<?>) entity;
                ce.key = TransactionCacheElement.generateKey((BaseEntity<?>) entity);
                ce.tx = transaction();

                transactionCache.put(ce.key, ce);
            }
            return true;
        }
        return false;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <E extends IEntity> E findEntity(@Nonnull Object key,
                                            @Nonnull Class<? extends E> type,
                                            Context context) throws
            DataStoreException {
        Preconditions.checkState(session != null);
        checkThread();

        E entity = session.find(type, key);
        if (entity instanceof BaseEntity) {
            ((BaseEntity) entity).getState().setState(EEntityState.Synced);
        }
        return entity;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query,
                                                            int offset,
                                                            int maxResults,
                                                            @Nonnull Class<? extends E> type,
                                                            Context context)
            throws DataStoreException {
        Preconditions.checkState(readSession != null);
        checkThread();
        Query qq = session.createQuery(query, type).setMaxResults(maxResults).setFirstResult(offset);
        List<E> result = qq.getResultList();
        if (result != null && !result.isEmpty()) {
            for (E entity : result) {
                if (entity instanceof BaseEntity) {
                    ((BaseEntity) entity).getState().setState(EEntityState.Synced);
                }
            }
            EntitySearchResult<E> er = new EntitySearchResult<>(type);
            er.setQuery(query);
            er.setOffset(offset);
            er.setCount(result.size());
            er.setEntities(result);
            return er;
        }
        return null;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query,
                                                            int offset, int maxResults,
                                                            Map<String, Object> parameters,
                                                            @Nonnull Class<? extends E> type,
                                                            Context context)
            throws DataStoreException {
        Preconditions.checkState(readSession != null);
        checkThread();

        Query qq = readSession.createQuery(query, type).setMaxResults(maxResults).setFirstResult(offset);
        if (parameters != null && !parameters.isEmpty()) {
            for (String key : parameters.keySet())
                qq.setParameter(key, parameters.get(key));
        }
        List<E> result = qq.getResultList();
        if (result != null && !result.isEmpty()) {
            for (E entity : result) {
                if (entity instanceof BaseEntity) {
                    ((BaseEntity) entity).getState().setState(EEntityState.Synced);
                }
            }
            EntitySearchResult<E> er = new EntitySearchResult<>(type);
            er.setQuery(query);
            er.setOffset(offset);
            er.setCount(result.size());
            er.setEntities((Collection<E>) result);
            return er;
        }
        return null;
    }

    @Override
    public DataStoreAuditContext context() {
        DataStoreAuditContext ctx = new DataStoreAuditContext();
        ctx.setType(getClass().getCanonicalName());
        ctx.setName(name());
        ctx.setConnectionType(connection().type().getCanonicalName());
        ctx.setConnectionName(connection().name());
        return ctx;
    }


    @Override
    public void configureDataStore(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        Preconditions.checkArgument(config() instanceof RdbmsConfig);

        try {
            AbstractConnection<Session> connection =
                    dataStoreManager.getConnection(config().getConnectionName(), Session.class);
            if (!(connection instanceof HibernateConnection)) {
                throw new ConfigurationException(String.format("No connection found for name. [name=%s]", config().getConnectionName()));
            }
            withConnection(connection);
            HibernateConnection hibernateConnection = (HibernateConnection) connection;
            session = hibernateConnection.connection();
            HibernateConnection readConnection = null;
            if (!Strings.isNullOrEmpty(((RdbmsConfig) config()).readConnectionName())) {
                AbstractConnection<Session> rc =
                        (AbstractConnection<Session>) dataStoreManager.getConnection(((RdbmsConfig) config()).readConnectionName(), Session.class);
                if (!(rc instanceof HibernateConnection)) {
                    throw new ConfigurationException(String.format("No connection found for name. [name=%s]", ((RdbmsConfig) config()).readConnectionName()));
                }
                readConnection = (HibernateConnection) rc;
            }
            if (readConnection != null) {
                readSession = readConnection.connection();
                readSession.setDefaultReadOnly(true);
            } else {
                readSession = session;
            }
            if (config().getMaxResults() > 0) {
                maxResults(config().getMaxResults());
            }
        } catch (ConnectionException | DataStoreException ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (session != null)
                connection().close(session);
            if (readConnection != null && readSession != null) {
                readConnection.close(readSession);
            }
            transactionCache.clear();

            session = null;
            readSession = null;
            super.close();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
