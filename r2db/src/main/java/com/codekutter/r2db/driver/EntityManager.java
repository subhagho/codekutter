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

package com.codekutter.r2db.driver;

import com.codekutter.common.Context;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.*;
import com.codekutter.common.stores.annotations.MappedStores;
import com.codekutter.common.stores.annotations.SchemaSharded;
import com.codekutter.common.stores.annotations.TableSharded;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.lucene.search.Query;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "entity-manager")
@SuppressWarnings("rawtypes")
public class EntityManager implements IConfigurable {
    @ConfigAttribute(name = "name")
    private String name;
    @Setter(AccessLevel.NONE)
    private DataStoreManager dataStoreManager;

    public <T extends IEntity> List<T> textSearch(@Nonnull Query query,
                                                  @Nonnull Class<? extends T> type,
                                                  Class<? extends AbstractDataStore<T>> storeType,
                                                  Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (!(dataStore instanceof ISearchable)) {
            throw new DataStoreException(String.format("Specified store type doesn't support text search. [data store=%s]", storeType.getCanonicalName()));
        }
        return ((ISearchable) dataStore).textSearch(query, type, context);
    }

    public <T extends IEntity> List<T> textSearch(@Nonnull Query query, int batchSize, int offset,
                                                  @Nonnull Class<? extends T> type,
                                                  Class<? extends AbstractDataStore<T>> storeType,
                                                  Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (!(dataStore instanceof ISearchable)) {
            throw new DataStoreException(String.format("Specified store type doesn't support text search. [data store=%s]", storeType.getCanonicalName()));
        }
        return ((ISearchable) dataStore).textSearch(query, batchSize, offset, type, context);
    }

    public <T extends IEntity> List<T> textSearch(@Nonnull String query,
                                                  @Nonnull Class<? extends T> type,
                                                  Class<? extends AbstractDataStore<T>> storeType,
                                                  Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (!(dataStore instanceof ISearchable)) {
            throw new DataStoreException(String.format("Specified store type doesn't support text search. [data store=%s]", storeType.getCanonicalName()));
        }
        return ((ISearchable) dataStore).textSearch(query, type, context);
    }

    public <T extends IEntity> List<T> textSearch(@Nonnull String query, int batchSize, int offset,
                                                  @Nonnull Class<? extends T> type,
                                                  Class<? extends AbstractDataStore<T>> storeType,
                                                  Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (!(dataStore instanceof ISearchable)) {
            throw new DataStoreException(String.format("Specified store type doesn't support text search. [data store=%s]", storeType.getCanonicalName()));
        }
        return ((ISearchable) dataStore).textSearch(query, batchSize, offset, type, context);
    }

    public <T> void beingTransaction(@Nonnull Class<? extends IEntity> type, Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if ((dataStore instanceof TransactionDataStore)) {
            if (!((TransactionDataStore) dataStore).isInTransaction()) {
                ((TransactionDataStore) dataStore).beingTransaction();
            }
        }
    }

    public <T> void commit(@Nonnull Class<? extends IEntity> type, Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if ((dataStore instanceof TransactionDataStore)) {
            if (!((TransactionDataStore) dataStore).isInTransaction()) {
                throw new DataStoreException(String.format("No active transaction. [thread id=%d][data store=%s]", Thread.currentThread().getId(), dataStore.name()));
            }
            ((TransactionDataStore) dataStore).commit();
        }
    }

    public <T> void rollback(Class<? extends IEntity> type, Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if ((dataStore instanceof TransactionDataStore)) {
            if (!((TransactionDataStore) dataStore).isInTransaction()) {
                throw new DataStoreException(String.format("No active transaction. [thread id=%d][data store=%s]", Thread.currentThread().getId(), dataStore.name()));
            }
            ((TransactionDataStore) dataStore).rollback();
        }
    }

    public <T, E extends IEntity> E create(@Nonnull E entity,
                                           @Nonnull Class<? extends IEntity> type,
                                           Class<? extends AbstractDataStore<T>> storeType,
                                           Context context) throws DataStoreException {
        Object shardKey = null;
        if (entity instanceof IShardedEntity) {
            shardKey = ((IShardedEntity) entity).getShardKey();
        } else if (type.isAnnotationPresent(TableSharded.class)) {

        }
        AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        return dataStore.create(entity, type, context);
    }

    public <T, E extends IEntity> E update(@Nonnull E entity,
                                           @Nonnull Class<? extends IEntity> type,
                                           Class<? extends AbstractDataStore<T>> storeType,
                                           Context context) throws DataStoreException {
        Object shardKey = null;
        if (entity instanceof IShardedEntity) {
            shardKey = ((IShardedEntity) entity).getShardKey();
        }
        AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        return dataStore.update(entity, type, context);
    }

    public <T, K, E extends IEntity<K>> boolean delete(@Nonnull E entity,
                                                       @Nonnull Class<? extends E> type,
                                                       Class<? extends AbstractDataStore<T>> storeType,
                                                       Context context) throws DataStoreException {
        Object shardKey = null;
        if (entity instanceof IShardedEntity) {
            shardKey = ((IShardedEntity) entity).getShardKey();
        }
        AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        return dataStore.delete(entity.getKey(), type, context);
    }

    public <T, E extends IEntity> E find(@Nonnull Object key, @Nonnull Class<? extends E> type,
                                         Class<? extends AbstractDataStore<T>> storeType,
                                         Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        return dataStore.find(key, type, context);
    }

    public <T, E extends IEntity> Collection<E> search(@Nonnull String query,
                                                       @Nonnull Class<? extends E> type,
                                                       Class<? extends AbstractDataStore<T>> storeType,
                                                       Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        return dataStore.search(query, type, context);
    }

    public <T, E extends IEntity> Collection<E> search(@Nonnull String query, int offset, int maxResults,
                                                       @Nonnull Class<? extends E> type,
                                                       Class<? extends AbstractDataStore<T>> storeType,
                                                       Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        return dataStore.search(query, offset, maxResults, type, context);
    }

    public <T, E extends IEntity> Collection<E> search(@Nonnull String query,
                                                       Map<String, Object> params,
                                                       @Nonnull Class<? extends E> type,
                                                       Class<? extends AbstractDataStore<T>> storeType,
                                                       Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        return dataStore.search(query, params, type, context);
    }

    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> Collection<E> search(Object shardKey,
                                                       @Nonnull String query,
                                                       Map<String, Object> params,
                                                       @Nonnull Class<? extends E> type,
                                                       Class<? extends AbstractDataStore<T>> storeType,
                                                       Context context) throws DataStoreException {
        if (!ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Not a sharded entity. [type=%s]", type.getCanonicalName()));
        }
        if (shardKey != null) {
            AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
            if (dataStore == null) {
                throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
            }
            return dataStore.search(query, params, type, context);
        } else {
            List<AbstractDataStore<T>> dataStores = dataStoreManager.getShards(storeType, (Class<? extends IShardedEntity>) type);
            List<E> values = new ArrayList<>();
            for (AbstractDataStore<T> dataStore : dataStores) {
                List<E> result = (List<E>) dataStore.search(query, params, type, context);
                if (result != null && !result.isEmpty()) {
                    values.addAll(result);
                }
            }
            if (!values.isEmpty())
                return values;
        }
        return null;
    }

    public <T, E extends IEntity> Collection<E> search(@Nonnull String query, int offset, int maxResults,
                                                       Map<String, Object> params,
                                                       @Nonnull Class<? extends E> type,
                                                       Class<? extends AbstractDataStore<T>> storeType,
                                                       Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        return dataStore.search(query, offset, maxResults, params, type, context);
    }

    public void close() throws IOException {
        try {
            dataStoreManager.closeStores();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public <T, E extends IEntity, K> AbstractDataStore<T> findStore(@Nonnull Class<? extends E> type,
                                                                    @Nonnull Class<? extends AbstractDataStore<T>> storetype,
                                                                    K shardKey) throws DataStoreException {
        AbstractDataStore<T> store = null;
        if (type.isAnnotationPresent(SchemaSharded.class)) {
            if (shardKey == null) {
                throw new DataStoreException(String.format("Shard Key not specified for sharded entity. [type=%s]", type.getCanonicalName()));
            }
            if (!ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
                throw new DataStoreException(String.format("Entity is marked as Schema Sharded but isn't a sharded entity. [type=%s]", type.getCanonicalName()));
            }
            Class<? extends IShardedEntity> se = (Class<? extends IShardedEntity>) type;
            store = dataStoreManager.getShard(storetype, se, shardKey);
        } else {
            store = findStore(type, storetype);
        }

        return store;
    }

    @SuppressWarnings("unchecked")
    private <T, E extends IEntity> AbstractDataStore<T> findStore(Class<? extends E> type,
                                                                  Class<? extends AbstractDataStore<T>> storetype) throws DataStoreException {
        if (storetype == null) {
            if (type.isAnnotationPresent(MappedStores.class)) {
                MappedStores stores = type.getAnnotation(MappedStores.class);
                if (stores != null) {
                    if (stores.stores().length == 1) {
                        storetype = (Class<? extends AbstractDataStore<T>>) stores.stores()[0];
                    } else {
                        throw new DataStoreException(String.format("Multiple store types specified for entity. [entity=%s]", type.getCanonicalName()));
                    }
                }
            }
            if (storetype == null) {
                throw new DataStoreException(String.format("No default store type found for entity. [entity=%s]", type.getCanonicalName()));
            }
        }
        return dataStoreManager.getDataStore(storetype, type);
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
        AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
        if (cnode == null) {
            throw new ConfigurationException(String.format("Configuration node not found. [node=%s]", node.getAbsolutePath()));
        }
        ConnectionManager.setup(cnode);

        dataStoreManager = new DataStoreManager();
        dataStoreManager.configure(cnode);
    }
}
