package com.codekutter.r2db.driver.impl;

import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.impl.HibernateConnection;
import com.codekutter.r2db.driver.DataStoreException;
import com.codekutter.r2db.driver.TransactionDataStore;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.annotation.Nonnull;
import javax.persistence.Query;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class RdbmsDataSource extends TransactionDataStore<Session, Transaction> {
    private HibernateConnection hibernateConnection = null;
    private Session session;

    @Override
    public void beingTransaction() throws DataStoreException {
        if (transaction() != null) {
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
        Preconditions.checkState(connection().state().isOpen());
        Preconditions.checkState(transaction() != null && transaction().isActive());
        checkThread();

        transaction().commit();
        transaction(null);
    }

    @Override
    public void rollback() throws DataStoreException {
        Preconditions.checkState(connection().state().isOpen());
        Preconditions.checkState(transaction() != null && transaction().isActive());
        checkThread();

        transaction().rollback();
        transaction(null);
    }

    @Override
    public <E extends IEntity> E create(@Nonnull E entity, @Nonnull Class<? extends IEntity> type) throws
                                                                                                   DataStoreException {
        Preconditions.checkState(connection().state().isOpen());
        Preconditions.checkState(transaction() != null && transaction().isActive());
        checkThread();

        Object result = session.save(entity);
        if (result == null) {
            throw new DataStoreException(String.format("Error saving entity. [type=%s][key=%s]", type.getCanonicalName(), entity.getKey()));
        }
        return entity;
    }

    @Override
    public <E extends IEntity> E update(@Nonnull E entity, @Nonnull Class<? extends IEntity> type) throws
                                                                                                   DataStoreException {
        Preconditions.checkState(connection().state().isOpen());
        Preconditions.checkState(transaction() != null && transaction().isActive());
        checkThread();

        Object result = session.save(entity);
        if (result == null) {
            throw new DataStoreException(String.format("Error updating entity. [type=%s][key=%s]", type.getCanonicalName(), entity.getKey()));
        }
        return entity;
    }

    @Override
    public <E extends IEntity> boolean delete(@Nonnull Object key, @Nonnull Class<? extends E> type) throws
                                                                                                     DataStoreException {
        Preconditions.checkState(connection().state().isOpen());
        Preconditions.checkState(transaction() != null && transaction().isActive());
        checkThread();

        E entity = find(key, type);
        if (entity != null) {
            session.delete(entity);
            return true;
        }
        return false;
    }

    @Override
    public <E extends IEntity> E find(@Nonnull Object key, @Nonnull Class<? extends E> type) throws
                                                                                             DataStoreException {
        Preconditions.checkState(connection().state().isOpen());
        checkThread();

        return session.find(type, key);
    }

    @Override
    public <E extends IEntity> Collection<E> search(@Nonnull String query,
                                                    int offset, int maxResults,
                                                    @Nonnull
                                                            Class<? extends E> type)
    throws DataStoreException {
        Preconditions.checkState(connection().state().isOpen());
        checkThread();

        Query qq = session.createQuery(query, type).setMaxResults(maxResults).setFirstResult(offset);

        return null;
    }

    @Override
    public <E extends IEntity> Collection<E> search(@Nonnull String query,
                                                    int offset, int maxResults,
                                                    Map<String, String> params,
                                                    @Nonnull
                                                            Class<? extends E> type)
    throws DataStoreException {
        return null;
    }


    @Override
    public void configure() throws ConfigurationException {
        Preconditions.checkArgument(connection() instanceof HibernateConnection);
        hibernateConnection = (HibernateConnection) connection();
        session = hibernateConnection.connection();
    }

    @Override
    public void close() throws IOException {
        session.close();
    }
}
