package com.codekutter.r2db.driver.impl;

import com.codekutter.r2db.driver.DataSourceException;
import com.codekutter.r2db.driver.TransactionDataSource;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;

public class RdbmsDataSource extends TransactionDataSource<Session, Transaction> {
    @Override
    public void beingTransaction() throws DataSourceException {
        if (transaction() != null) {
            if (connection().connection().isJoinedToTransaction()) {
                throw new DataSourceException("Session already has a running transaction.");
            }
            transaction(connection().connection().beginTransaction());
        } else if (!connection().connection().isJoinedToTransaction()) {
            throw new DataSourceException("Transaction handle is set but session has no active transaction.");
        }
    }

    @Override
    public void commit() throws DataSourceException {
        Preconditions.checkState(connection().state().isOpen());
        Preconditions.checkState(transaction() != null && transaction().isActive());

        transaction().commit();
        transaction(null);
    }

    @Override
    public void rollback() throws DataSourceException {
        Preconditions.checkState(connection().state().isOpen());
        Preconditions.checkState(transaction() != null && transaction().isActive());

        transaction().rollback();
        transaction(null);
    }

    @Override
    public <E extends IEntity> E create(@NonNull E entity, @NonNull Class<? extends IEntity> type) throws DataSourceException {
        Preconditions.checkState(connection().state().isOpen());
        Preconditions.checkState(transaction() != null && transaction().isActive());

        Object rslt = connection().connection().save(entity);
        if (rslt == null) {
            throw new DataSourceException(String.format("Error saving entity. [type=%s][key=%s]", type.getCanonicalName(), entity.getKey()));
        }
        return entity;
    }

    @Override
    public <E extends IEntity> E update(@NonNull E entity, @NonNull Class<? extends IEntity> type) throws DataSourceException {
        Preconditions.checkState(connection().state().isOpen());
        Preconditions.checkState(transaction() != null && transaction().isActive());

        Object rslt = connection().connection().save(entity);
        if (rslt == null) {
            throw new DataSourceException(String.format("Error updating entity. [type=%s][key=%s]", type.getCanonicalName(), entity.getKey()));
        }
        return entity;
    }

    @Override
    public <E extends IEntity> boolean delete(@NonNull Object key, @NonNull Class<? extends E> type) throws DataSourceException {
        return false;
    }

    @Override
    public <E extends IEntity> E find(@NonNull Object key, @NonNull Class<? extends E> type) throws DataSourceException {
        return null;
    }

    @Override
    public <E extends IEntity> Collection<E> search(String query, @NonNull Class<? extends E> type) throws DataSourceException {
        return null;
    }

    @Override
    public <E extends IEntity> Collection<E> search(String query, Map<String, String> params, @NonNull Class<? extends E> type) throws DataSourceException {
        return null;
    }

    @Override
    public void configure(@Nonnull AbstractConfigNode abstractConfigNode) throws ConfigurationException {

    }
}
