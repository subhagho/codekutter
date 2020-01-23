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
import com.codekutter.common.auditing.AuditManager;
import com.codekutter.common.auditing.Audited;
import com.codekutter.common.auditing.IChange;
import com.codekutter.common.model.AuditRecord;
import com.codekutter.common.model.EAuditType;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.IKey;
import com.codekutter.common.stores.*;
import com.codekutter.common.stores.annotations.*;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;

import javax.annotation.Nonnull;
import javax.persistence.JoinColumn;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.Principal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "entity-manager")
@SuppressWarnings("rawtypes")
public class EntityManager implements IConfigurable {
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final String KEY_SEPARATOR = "~||~";

    @ConfigAttribute(name = "name")
    private String name;
    @Setter(AccessLevel.NONE)
    private DataStoreManager dataStoreManager;
    @Setter(AccessLevel.NONE)
    private Map<Class<? extends IShardProvider>, IShardProvider> shardProviders = new ConcurrentHashMap<>();

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

    public void commit() throws DataStoreException {
        dataStoreManager.commit();
    }

    public void rollback() throws DataStoreException {
        dataStoreManager.rollback();
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

    public void closeStores() throws DataStoreException{
        dataStoreManager.closeStores();
    }

    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> E create(@Nonnull E entity,
                                           @Nonnull Class<? extends IEntity> type,
                                           Class<? extends AbstractDataStore<T>> storeType,
                                           @Nonnull Principal user,
                                           Context context) throws DataStoreException {
        try {
            Object shardKey = null;
            if (entity instanceof IShardedEntity) {
                shardKey = ((IShardedEntity) entity).getShardKey();
            }
            AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
            if (dataStore == null) {
                throw new DataStoreException(String.format("No data store found for entity. [type=%s]",
                        type.getCanonicalName()));
            }
            entity = (E) formatEntity(entity, context);
            entity = dataStore.create(entity, type, context);
            if (type.isAnnotationPresent(Audited.class)) {
                AuditRecord record = AuditManager.get().audit(EAuditType.Create, entity, null, user);
                if (record == null) {
                    LogUtils.error(getClass(), String.format("Audit Log failed for type. [type=%s]", type.getCanonicalName()));
                }
            }

            return createReferences(entity, type, user, context);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private <E extends IEntity> E createReferences(E entity,
                                                   Class<? extends IEntity> entityType,
                                                   Principal user,
                                                   Context context) throws DataStoreException {
        try {
            List<Field> fields = getReferenceFields(entityType);
            if (fields != null && !fields.isEmpty()) {
                for (Field f : fields) {
                    Object value = ReflectionUtils.getFieldValue(entity, f);
                    if (value != null) {
                        Class<?> type = f.getType();
                        if (ReflectionUtils.implementsInterface(List.class, type)) {
                            type = ReflectionUtils.getGenericListType(f);
                            Collection values = (Collection) value;
                            for (Object v : values) {
                                Object t = create((IEntity) v, (Class<? extends IEntity>) type, null, user, context);
                                if (t == null) {
                                    throw new DataStoreException(
                                            String.format("Error creating nested entity. [type=%s][key=%s]",
                                                    type.getCanonicalName(), ((IEntity) v).getKey().stringKey()));
                                }
                            }
                        } else if (ReflectionUtils.implementsInterface(Set.class, type)) {
                            type = ReflectionUtils.getGenericSetType(f);
                            Collection values = (Collection) value;
                            for (Object v : values) {
                                Object t = create((IEntity) v, (Class<? extends IEntity>) type, null, user, context);
                                if (t == null) {
                                    throw new DataStoreException(
                                            String.format("Error creating nested entity. [type=%s][key=%s]",
                                                    type.getCanonicalName(), ((IEntity) v).getKey().stringKey()));
                                }
                            }
                        } else {
                            Object t = create((IEntity) value, (Class<? extends IEntity>) type, null, user, context);
                            if (t == null) {
                                throw new DataStoreException(
                                        String.format("Error creating nested entity. [type=%s][key=%s]",
                                                type.getCanonicalName(), ((IEntity) value).getKey().stringKey()));
                            }
                        }
                    }
                }
            }
            return entity;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private List<Field> getReferenceFields(Class<? extends IEntity> entityType) throws DataStoreException {
        try {
            List<Field> fields = null;
            Field[] source = ReflectionUtils.getAllFields(entityType);
            for (Field f : source) {
                if (f.isAnnotationPresent(Reference.class)) {
                    Class<?> type = f.getType();
                    if (ReflectionUtils.implementsInterface(List.class, type)) {
                        type = ReflectionUtils.getGenericListType(f);
                    } else if (ReflectionUtils.implementsInterface(Set.class, type)) {
                        type = ReflectionUtils.getGenericSetType(f);
                    }
                    if (!ReflectionUtils.implementsInterface(IEntity.class, type)) {
                        throw new DataStoreException(
                                String.format("Invalid reference definition. [type=%s][field=%s][field type=%s]",
                                        entityType.getCanonicalName(), f.getName(), type.getCanonicalName()));
                    }
                    if (fields == null) {
                        fields = new ArrayList<>();
                    }
                    fields.add(f);
                }
            }
            return fields;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> E update(@Nonnull E entity,
                                           @Nonnull Class<? extends IEntity> type,
                                           Class<? extends AbstractDataStore<T>> storeType,
                                           @Nonnull Principal user,
                                           Context context) throws DataStoreException {
        try {
            Object shardKey = null;
            if (entity instanceof IShardedEntity) {
                shardKey = ((IShardedEntity) entity).getShardKey();
            }
            AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
            if (dataStore == null) {
                throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
            }
            E prev = null;
            if (type.isAnnotationPresent(Audited.class)) {
                prev = (E) find(entity.getKey(), type, shardKey, storeType, context);
                if (prev == null) {
                    throw new DataStoreException(String.format("Current entity record not found. [type=%s][key=%s]",
                            type.getCanonicalName(), entity.getKey().stringKey()));
                }
            }
            entity = (E) formatEntity(entity, context);
            entity = dataStore.update(entity, type, context);
            if (type.isAnnotationPresent(Audited.class)) {
                String delta = null;
                if (entity instanceof IChange) {
                    JsonNode node = ((IChange) entity).getChange(prev);
                    if (node != null) {
                        delta = node.toPrettyString();
                    }
                }
                AuditRecord record = AuditManager.get().audit(EAuditType.Update, entity, delta, user);
                if (record == null) {
                    LogUtils.error(getClass(), String.format("Audit Log failed for type. [type=%s]", type.getCanonicalName()));
                }
            }
            return updateReferences(entity, type, user, context);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private <E extends IEntity> E updateReferences(E entity,
                                                   Class<? extends IEntity> entityType,
                                                   Principal user,
                                                   Context context) throws DataStoreException {
        try {
            List<Field> fields = getReferenceFields(entityType);
            if (fields != null && !fields.isEmpty()) {
                for (Field f : fields) {
                    Object value = ReflectionUtils.getFieldValue(entity, f);
                    if (value != null) {
                        Class<?> type = f.getType();
                        if (ReflectionUtils.implementsInterface(List.class, type)) {
                            type = ReflectionUtils.getGenericListType(f);
                            Collection values = (Collection) value;
                            for (Object v : values) {
                                Object t = create((IEntity) v, (Class<? extends IEntity>) type, null, user, context);
                                if (t == null) {
                                    throw new DataStoreException(
                                            String.format("Error creating nested entity. [type=%s][key=%s]",
                                                    type.getCanonicalName(), ((IEntity) v).getKey().stringKey()));
                                }
                            }
                        } else if (ReflectionUtils.implementsInterface(Set.class, type)) {
                            type = ReflectionUtils.getGenericSetType(f);
                            Collection values = (Collection) value;
                            for (Object v : values) {
                                Object t = create((IEntity) v, (Class<? extends IEntity>) type, null, user, context);
                                if (t == null) {
                                    throw new DataStoreException(
                                            String.format("Error creating nested entity. [type=%s][key=%s]",
                                                    type.getCanonicalName(), ((IEntity) v).getKey().stringKey()));
                                }
                            }
                        } else {
                            Object t = create((IEntity) value, (Class<? extends IEntity>) type, null, user, context);
                            if (t == null) {
                                throw new DataStoreException(
                                        String.format("Error creating nested entity. [type=%s][key=%s]",
                                                type.getCanonicalName(), ((IEntity) value).getKey().stringKey()));
                            }
                        }
                    }
                }
            }
            return entity;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <T, K extends IKey, E extends IEntity<K>> boolean delete(@Nonnull E entity,
                                                                    @Nonnull Class<? extends E> type,
                                                                    Class<? extends AbstractDataStore<T>> storeType,
                                                                    @Nonnull Principal user,
                                                                    Context context) throws DataStoreException {
        try {
            Object shardKey = null;
            if (entity instanceof IShardedEntity) {
                shardKey = ((IShardedEntity) entity).getShardKey();
            }
            AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
            if (dataStore == null) {
                throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
            }
            E prev = null;
            if (type.isAnnotationPresent(Audited.class)) {
                prev = (E) find(entity.getKey(), type, shardKey, storeType, context);
                if (prev == null) {
                    throw new DataStoreException(String.format("Current entity record not found. [type=%s][key=%s]",
                            type.getCanonicalName(), entity.getKey().stringKey()));
                }
            }
            entity = (E) formatEntity(entity, context);
            boolean ret = dataStore.delete(entity.getKey(), type, context);
            if (type.isAnnotationPresent(Audited.class)) {
                AuditRecord record = AuditManager.get().audit(EAuditType.Delete, entity, null, user);
                if (record == null) {
                    LogUtils.error(getClass(), String.format("Audit Log failed for type. [type=%s]", type.getCanonicalName()));
                }
            }
            return ret;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private <K extends IKey, E extends IEntity<K>> E formatEntity(@Nonnull E entity, Context context) throws DataStoreException {
        try {
            Class<? extends E> type = (Class<? extends E>) entity.getClass();
            if (entity instanceof IShardedEntity) {
                if (type.isAnnotationPresent(TableSharded.class)) {
                    Object shardKey = ((IShardedEntity) entity).getShardKey();
                    if (shardKey == null) {
                        throw new DataStoreException(String.format("Shard Key is null. [type=%s][key=%s]",
                                type.getCanonicalName(), entity.getKey().stringKey()));
                    }
                    TableSharded ts = type.getAnnotation(TableSharded.class);
                    IShardProvider provider = getShardProvider(ts.provider());
                    int shard = provider.getShard(shardKey);
                    Class<? extends E> ctype = null;
                    for (TableShardSpec spec : ts.specs()) {
                        if (spec.shard() == shard) {
                            ctype = (Class<? extends E>) spec.mappedEntity();
                            break;
                        }
                    }
                    if (ctype == null) {
                        throw new DataStoreException(String.format("Shard definition not found. [type=%s][shard=%d]",
                                type.getCanonicalName(), shard));
                    }
                    E ne = ctype.newInstance();
                    entity = (E) ne.copyChanges(entity, context);
                }
            }
            return entity;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private @Nonnull
    IShardProvider getShardProvider(Class<? extends IShardProvider> type) throws DataStoreException {
        try {
            if (!shardProviders.containsKey(type)) {
                IShardProvider provider = type.newInstance();
                shardProviders.put(type, provider);
            }
            return shardProviders.get(type);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <T, E extends IEntity> E find(@Nonnull Object key,
                                         @Nonnull Class<? extends E> type,
                                         Class<? extends AbstractDataStore<T>> storeType,
                                         Context context) throws DataStoreException {
        Preconditions.checkArgument(!type.isAnnotationPresent(SchemaSharded.class));
        return find(key, type, null, storeType, context);
    }

    public <T, E extends IEntity> E find(@Nonnull Object key,
                                         @Nonnull Class<? extends E> type,
                                         Object shardKey,
                                         Class<? extends AbstractDataStore<T>> storeType,
                                         Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        E value = dataStore.find(key, type, context);
        if (value != null)
            findReferences(value, type, context);
        return value;
    }

    public <T, E extends IEntity> Collection<E> search(@Nonnull String query,
                                                       @Nonnull Class<? extends E> type,
                                                       Class<? extends AbstractDataStore<T>> storeType,
                                                       Context context) throws DataStoreException {
        Preconditions.checkArgument(!type.isAnnotationPresent(SchemaSharded.class));
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        Collection<E> values = dataStore.search(query, type, context);
        if (!values.isEmpty())
            return findReferences(values, type, context);
        return null;
    }

    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> Collection<E> search(Object shardKey,
                                                       @Nonnull String query,
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
            Collection<E> values = dataStore.search(query, type, context);
            if (!values.isEmpty())
                return findReferences(values, type, context);
        } else {
            List<AbstractDataStore<T>> dataStores = dataStoreManager.getShards(storeType, (Class<? extends IShardedEntity>) type);
            List<E> values = new ArrayList<>();
            for (AbstractDataStore<T> dataStore : dataStores) {
                List<E> result = (List<E>) dataStore.search(query, type, context);
                if (result != null && !result.isEmpty()) {
                    values.addAll(result);
                }
            }
            if (!values.isEmpty())
                return findReferences(values, type, context);
        }
        return null;
    }

    public <T, E extends IEntity> Collection<E> search(@Nonnull String query,
                                                       int offset,
                                                       int maxResults,
                                                       @Nonnull Class<? extends E> type,
                                                       Class<? extends AbstractDataStore<T>> storeType,
                                                       Context context) throws DataStoreException {
        Preconditions.checkArgument(!type.isAnnotationPresent(SchemaSharded.class));
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        Collection<E> values = dataStore.search(query, offset, maxResults, type, context);
        if (!values.isEmpty())
            return findReferences(values, type, context);
        return null;
    }


    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> Collection<E> search(Object shardKey,
                                                       @Nonnull String query,
                                                       int offset,
                                                       int maxResults,
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
            Collection<E> values = dataStore.search(query, offset, maxResults, type, context);
            if (!values.isEmpty())
                return findReferences(values, type, context);
        } else {
            List<AbstractDataStore<T>> dataStores = dataStoreManager.getShards(storeType, (Class<? extends IShardedEntity>) type);
            List<E> values = new ArrayList<>();
            for (AbstractDataStore<T> dataStore : dataStores) {
                List<E> result = (List<E>) dataStore.search(query, offset, maxResults, type, context);
                if (result != null && !result.isEmpty()) {
                    values.addAll(result);
                }
            }
            if (!values.isEmpty())
                return findReferences(values, type, context);
        }
        return null;
    }

    public <T, E extends IEntity> Collection<E> search(@Nonnull String query,
                                                       Map<String, Object> params,
                                                       @Nonnull Class<? extends E> type,
                                                       Class<? extends AbstractDataStore<T>> storeType,
                                                       Context context) throws DataStoreException {
        Preconditions.checkArgument(!type.isAnnotationPresent(SchemaSharded.class));
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        Collection<E> values = dataStore.search(query, params, type, context);
        if (!values.isEmpty())
            return findReferences(values, type, context);
        return null;
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
            Collection<E> values = dataStore.search(query, params, type, context);
            if (!values.isEmpty())
                return findReferences(values, type, context);
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
                return findReferences(values, type, context);
        }
        return null;
    }

    public <T, E extends IEntity> Collection<E> search(@Nonnull String query,
                                                       int offset,
                                                       int maxResults,
                                                       Map<String, Object> params,
                                                       @Nonnull Class<? extends E> type,
                                                       Class<? extends AbstractDataStore<T>> storeType,
                                                       Context context) throws DataStoreException {
        Preconditions.checkArgument(!type.isAnnotationPresent(SchemaSharded.class));
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        Collection<E> values = dataStore.search(query, offset, maxResults, params, type, context);
        if (!values.isEmpty())
            return findReferences(values, type, context);
        return null;
    }


    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> Collection<E> search(Object shardKey,
                                                       @Nonnull String query,
                                                       int offset,
                                                       int maxResults,
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
            Collection<E> values = dataStore.search(query, offset, maxResults, params, type, context);
            if (!values.isEmpty())
                return findReferences(values, type, context);
        } else {
            List<AbstractDataStore<T>> dataStores = dataStoreManager.getShards(storeType, (Class<? extends IShardedEntity>) type);
            List<E> values = new ArrayList<>();
            for (AbstractDataStore<T> dataStore : dataStores) {
                List<E> result = (List<E>) dataStore.search(query, offset, maxResults, params, type, context);
                if (result != null && !result.isEmpty()) {
                    values.addAll(result);
                }
            }
            if (!values.isEmpty())
                return findReferences(values, type, context);
        }
        return null;
    }


    @SuppressWarnings("unchecked")
    private <E extends IEntity> Collection<E> findReferences(Collection<E> entities,
                                                             @Nonnull Class<? extends E> entityType,
                                                             Context context) throws DataStoreException {
        try {
            List<Field> fields = getReferenceFields(entityType);
            if (fields != null && !fields.isEmpty()) {
                for (Field f : fields) {
                    Reference reference = f.getAnnotation(Reference.class);
                    String query = JoinPredicateHelper.generateHibernateJoinQuery(reference, entities, f, dataStoreManager);
                    if (Strings.isNullOrEmpty(query)) {
                        throw new DataStoreException(String.format("NULL query returned. [type=%s][field=%s]",
                                entityType.getCanonicalName(), f.getName()));
                    }
                    int offset = 0;
                    Multimap<String, E> parentMap = (Multimap<String, E>) mapCollection(entities, reference, true);
                    while (true) {
                        Collection result = search(query,
                                offset,
                                DEFAULT_BATCH_SIZE,
                                (Class<? extends IEntity>) f.getType(),
                                null, context);
                        if (result != null && !result.isEmpty()) {
                            joinResults(parentMap, result, f, entityType, reference);
                        }
                        if (result == null || result.size() < DEFAULT_BATCH_SIZE) break;
                        offset += result.size();
                    }
                }
            }
            return entities;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private <E extends IEntity> void joinResults(Multimap<String, E> sources,
                                                 Collection result,
                                                 Field field,
                                                 Class<? extends IEntity> entityType,
                                                 Reference reference) throws Exception {
        Multimap<String, IEntity> resultMap = mapCollection(result, reference, false);
        for (String key : resultMap.keySet()) {
            if (!sources.containsKey(key)) {
                throw new DataStoreException(String.format("Join key invalid. [type=%s][field=%s][key=%s]",
                        entityType.getCanonicalName(),
                        field.getName(), key));
            }
            Collection<IEntity> values = resultMap.get(key);
            Collection<E> parents = sources.get(key);
            for (E parent : parents) {
                Class<?> type = field.getType();
                if (ReflectionUtils.implementsInterface(List.class, type)) {
                    List vs = new ArrayList(values);
                    ReflectionUtils.setObjectValue(parent, field, vs);
                } else if (ReflectionUtils.implementsInterface(Set.class, type)) {
                    Set vs = new HashSet(values);
                    ReflectionUtils.setObjectValue(parent, field, vs);
                } else {
                    while (values.iterator().hasNext()) {
                        Object o = values.iterator().next();
                        ReflectionUtils.setObjectValue(parent, field, o);
                        break;
                    }
                }
            }
        }
    }

    private Multimap<String, IEntity> mapCollection(Collection result,
                                                    Reference reference,
                                                    boolean parent) throws Exception {
        Multimap<String, IEntity> values = ArrayListMultimap.create();
        for (Object r : result) {
            Preconditions.checkArgument(r instanceof IEntity);
            String key = getJoinValue(r, reference, parent);
            values.put(key, (IEntity) r);
        }
        return values;
    }

    private String getJoinValue(Object value, Reference reference, boolean parent) throws Exception {
        StringBuffer buff = new StringBuffer();
        for (JoinColumn column : reference.columns().value()) {
            String cname = (parent ? column.name() : column.referencedColumnName());
            Object v = ReflectionUtils.getNestedFieldValue(value, cname);
            if (buff.length() > 0) {
                buff.append(KEY_SEPARATOR);
            }
            if (v != null) {
                buff.append(v);
            } else {
                buff.append("NULL");
            }
        }
        return buff.toString();
    }

    @SuppressWarnings("unchecked")
    private <E extends IEntity> E findReferences(E entity,
                                                 @Nonnull Class<? extends E> entityType,
                                                 Context context) throws DataStoreException {
        try {
            List<Field> fields = getReferenceFields(entityType);
            if (fields != null && !fields.isEmpty()) {
                for (Field f : fields) {
                    Reference reference = f.getAnnotation(Reference.class);
                    String query = JoinPredicateHelper.generateHibernateJoinQuery(reference, entity, f, dataStoreManager);
                    if (Strings.isNullOrEmpty(query)) {
                        throw new DataStoreException(String.format("NULL query returned. [type=%s][field=%s]",
                                entityType.getCanonicalName(), f.getName()));
                    }
                    Collection result = search(query, (Class<? extends IEntity>) reference.target(), null, context);
                    if (result != null && !result.isEmpty()) {
                        Class<?> type = f.getType();
                        if (ReflectionUtils.implementsInterface(List.class, type)) {
                            List values = new ArrayList(result);
                            ReflectionUtils.setObjectValue(entity, f, values);
                        } else if (ReflectionUtils.implementsInterface(Set.class, type)) {
                            Set values = new HashSet(result);
                            ReflectionUtils.setObjectValue(entity, f, values);
                        } else {
                            while (result.iterator().hasNext()) {
                                Object o = result.iterator().next();
                                ReflectionUtils.setObjectValue(entity, f, o);
                                break;
                            }
                        }
                    }
                }
            }
            return entity;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
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
