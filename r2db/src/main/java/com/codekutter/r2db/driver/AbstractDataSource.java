package com.codekutter.r2db.driver;

import com.codekutter.zconfig.common.IConfigurable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AbstractDataSource<T> implements IConfigurable {
    @Setter(AccessLevel.NONE)
    private AbstractConnection<T> connection = null;
    @Setter(AccessLevel.NONE)
    private long threadId;

    public AbstractDataSource() {
        threadId = Thread.currentThread().getId();
    }

    protected boolean checkThread() {
        long threadId = Thread.currentThread().getId();
        return this.threadId == threadId;
    }

    public AbstractDataSource<T> withconnection(@NonNull AbstractConnection<T> connection) {
        this.connection = connection;
        return this;
    }

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> E create(@NonNull E entity, @NonNull Class<? extends IEntity> type) throws DataSourceException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> E update(@NonNull E entity, @NonNull Class<? extends IEntity> type) throws DataSourceException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> boolean delete(@NonNull Object key, @NonNull Class<? extends E> type) throws DataSourceException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> E find(@NonNull Object key, @NonNull Class<? extends E> type) throws DataSourceException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> Collection<E> search(String query, @NonNull Class<? extends E> type) throws DataSourceException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> Collection<E> search(String query, Map<String, String> params, @NonNull Class<? extends E> type) throws DataSourceException;
}
