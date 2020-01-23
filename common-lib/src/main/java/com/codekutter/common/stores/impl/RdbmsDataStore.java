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
import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.*;
import com.codekutter.common.stores.impl.HibernateConnection;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.annotation.Nonnull;
import javax.persistence.Query;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RdbmsDataStore extends TransactionDataStore<Session, Transaction> {
    private HibernateConnection readConnection = null;
    protected Session session;
    protected Session readSession;

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

    @Override
    public void commit() throws DataStoreException {
        Preconditions.checkState(session != null);
        Preconditions.checkState(isInTransaction());
        checkThread();

        transaction().commit();
        transaction(null);
    }

    @Override
    public void rollback() throws DataStoreException {
        Preconditions.checkState(session != null);
        Preconditions.checkState(isInTransaction());
        checkThread();

        transaction().rollback();
        transaction(null);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <E extends IEntity> E create(@Nonnull E entity,
                                        @Nonnull Class<? extends IEntity> type,
                                        Context context) throws
            DataStoreException {
        Preconditions.checkState(session != null);
        Preconditions.checkState(isInTransaction());
        checkThread();

        Object result = session.save(entity);
        if (result == null) {
            throw new DataStoreException(String.format("Error saving entity. [type=%s][key=%s]", type.getCanonicalName(), entity.getKey()));
        }
        return entity;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <E extends IEntity> E update(@Nonnull E entity,
                                        @Nonnull Class<? extends IEntity> type,
                                        Context context) throws
            DataStoreException {
        Preconditions.checkState(session != null);
        Preconditions.checkState(isInTransaction());
        checkThread();

        Object result = session.save(entity);
        if (result == null) {
            throw new DataStoreException(String.format("Error updating entity. [type=%s][key=%s]", type.getCanonicalName(), entity.getKey()));
        }
        return entity;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <E extends IEntity> boolean delete(@Nonnull Object key,
                                              @Nonnull Class<? extends E> type,
                                              Context context) throws
            DataStoreException {
        Preconditions.checkState(session != null);
        Preconditions.checkState(isInTransaction());
        checkThread();

        E entity = find(key, type, context);
        if (entity != null) {
            session.delete(entity);
            return true;
        }
        return false;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <E extends IEntity> E find(@Nonnull Object key,
                                      @Nonnull Class<? extends E> type,
                                      Context context) throws
            DataStoreException {
        Preconditions.checkState(session != null);
        checkThread();

        return readSession.find(type, key);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E extends IEntity> Collection<E> search(@Nonnull String query,
                                                    int offset, int maxResults,
                                                    @Nonnull Class<? extends E> type,
                                                    Context context)
            throws DataStoreException {
        Preconditions.checkState(readSession != null);
        checkThread();
        query = String.format("FROM %s WHERE (%s)", type.getCanonicalName(), query);
        Query qq = session.createQuery(query, type).setMaxResults(maxResults).setFirstResult(offset);
        List<?> result = qq.getResultList();
        if (result != null && !result.isEmpty()) {
            return (Collection<E>) result;
        }
        return null;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E extends IEntity> Collection<E> search(@Nonnull String query,
                                                    int offset, int maxResults,
                                                    Map<String, Object> parameters,
                                                    @Nonnull Class<? extends E> type,
                                                    Context context)
            throws DataStoreException {
        Preconditions.checkState(readSession != null);
        checkThread();

        query = String.format("FROM %s WHERE (%s)", type.getCanonicalName(), query);
        Query qq = readSession.createQuery(query, type).setMaxResults(maxResults).setFirstResult(offset);
        if (parameters != null && !parameters.isEmpty()) {
            for (String key : parameters.keySet())
                qq.setParameter(key, parameters.get(key));
        }
        List<?> result = qq.getResultList();
        if (result != null && !result.isEmpty()) {
            return (Collection<E>) result;
        }
        return null;
    }


    @Override
    public void configure(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        Preconditions.checkArgument(config() instanceof RdbmsConfig);

        AbstractConnection<Session> connection =
                dataStoreManager.getConnection(config().connectionName(), Session.class);
        if (!(connection instanceof HibernateConnection)) {
            throw new ConfigurationException(String.format("No connection found for name. [name=%s]", config().connectionName()));
        }
        withConnection(connection);
        try {
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
        } catch (ConnectionException ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        session.close();
        if (readConnection != null) {
            readSession.close();
            readConnection = null;
        }
        session = null;
        readSession = null;
    }
}
