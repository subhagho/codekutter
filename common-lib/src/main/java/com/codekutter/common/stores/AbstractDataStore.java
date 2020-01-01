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

package com.codekutter.common.stores;

import com.codekutter.common.model.IEntity;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AbstractDataStore<T> implements Closeable {
    private static final int DEFAULT_MAX_RESULTS = 500;

    @ConfigAttribute(name = "name", required = true)
    private String name;
    @ConfigValue(name = "maxResults")
    private int maxResults = DEFAULT_MAX_RESULTS;
    @Setter(AccessLevel.NONE)
    private AbstractConnection<T> connection = null;
    @Setter(AccessLevel.NONE)
    private long threadId;
    @Setter(AccessLevel.NONE)
    private DataStoreConfig config;

    public AbstractDataStore() {
        threadId = Thread.currentThread().getId();
    }

    protected boolean checkThread() {
        long threadId = Thread.currentThread().getId();
        return this.threadId == threadId;
    }

    public AbstractDataStore<T> withConfig(@Nonnull DataStoreConfig config) {
        this.config = config;
        return this;
    }

    public AbstractDataStore<T> withConnection(@Nonnull AbstractConnection<T> connection) {
        this.connection = connection;
        return this;
    }

    public abstract void configure(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> E create(@Nonnull E entity, @Nonnull Class<? extends IEntity> type) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> E update(@Nonnull E entity, @Nonnull Class<? extends IEntity> type) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> boolean delete(@Nonnull Object key, @Nonnull Class<? extends E> type) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> E find(@Nonnull Object key, @Nonnull Class<? extends E> type) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> Collection<E> search(@Nonnull String query, int offset, int maxResults, @Nonnull Class<? extends E> type) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> Collection<E> search(@Nonnull String query, int offset, int maxResults, Map<String, Object> params, @Nonnull Class<? extends E> type) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public <E extends IEntity> Collection<E> search(@Nonnull String query, @Nonnull Class<? extends E> type) throws
            DataStoreException {
        return search(query, 0, maxResults, type);
    }

    @SuppressWarnings("rawtypes")
    public <E extends IEntity> Collection<E> search(@Nonnull String query, Map<String, String> params, @Nonnull Class<? extends E> type) throws
            DataStoreException {
        return search(query, 0, maxResults, type);
    }

}
