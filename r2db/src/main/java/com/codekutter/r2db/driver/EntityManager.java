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
import com.codekutter.common.auditing.*;
import com.codekutter.common.model.AuditRecord;
import com.codekutter.common.model.EAuditType;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.IKey;
import com.codekutter.common.stores.*;
import com.codekutter.common.stores.annotations.*;
import com.codekutter.common.stores.impl.EntitySearchResult;
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
@SuppressWarnings({"rawtypes", "unchecked"})
public class EntityManager implements IConfigurable {
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final String KEY_SEPARATOR = "~||~";

    @ConfigAttribute(name = "name")
    private String name;
    @Setter(AccessLevel.NONE)
    private DataStoreManager dataStoreManager;
    @Setter(AccessLevel.NONE)
    private Map<Class<? extends IShardProvider>, IShardProvider> shardProviders = new ConcurrentHashMap<>();

    public <T, E extends IEntity> BaseSearchResult<E> textSearch(@Nonnull Query query,
                                                  @Nonnull Class<? extends E> type,
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

    public <T, E extends IEntity> BaseSearchResult<E> textSearch(@Nonnull Query query, int batchSize, int offset,
                                                  @Nonnull Class<? extends E> type,
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

    public <T, E extends IEntity> BaseSearchResult<E> textSearch(@Nonnull String query,
                                                  @Nonnull Class<? extends E> type,
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

    public <T, E extends IEntity> BaseSearchResult<E> textSearch(@Nonnull String query, int batchSize, int offset,
                                                  @Nonnull Class<? extends E> type,
                                                  Class<? extends AbstractDataStore<T>> storeType,
                                                  Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = (AbstractDataStore<T>) findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (!(dataStore instanceof ISearchable)) {
            throw new DataStoreException(String.format("Specified store type doesn't support text search. [data store=%s]", storeType.getCanonicalName()));
        }
        return ((ISearchable) dataStore).textSearch(query, batchSize, offset, type, context);
    }

    public <T, E extends IEntity> BaseSearchResult<E> facetedSearch(@Nonnull Object query,
                                                                 @Nonnull Class<? extends E> type,
                                                                 Class<? extends AbstractDataStore<T>> storeType,
                                                                 Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = (AbstractDataStore<T>) findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (!(dataStore instanceof ISearchable)) {
            throw new DataStoreException(String.format("Specified store type doesn't support text search. [data store=%s]", storeType.getCanonicalName()));
        }
        return ((ISearchable) dataStore).facetedSearch(query, type, context);
    }

    public <T> void beingTransaction(@Nonnull Class<? extends IEntity> type, Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        beingTransaction(dataStore);
    }

    public <T> void beingTransaction(@Nonnull AbstractDataStore<T> dataStore) throws DataStoreException {
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
        try {
            AbstractDataStore<T> dataStore = findStore(type, storeType);
            if (dataStore == null) {
                throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
            }
            if (dataStore.auditLogger() != null) {
                dataStore.auditLogger().flush();
            }
            if ((dataStore instanceof TransactionDataStore)) {
                if (!((TransactionDataStore) dataStore).isInTransaction()) {
                    throw new DataStoreException(String.format("No active transaction. [thread id=%d][data store=%s]", Thread.currentThread().getId(), dataStore.name()));
                }
                ((TransactionDataStore) dataStore).commit();
            }
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public <T> void rollback(Class<? extends IEntity> type, Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {
        try {
            AbstractDataStore<T> dataStore = findStore(type, storeType);
            if (dataStore == null) {
                throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
            }
            if (dataStore.auditLogger() != null) {
                dataStore.auditLogger().discard();
            }
            if ((dataStore instanceof TransactionDataStore)) {
                if (!((TransactionDataStore) dataStore).isInTransaction()) {
                    throw new DataStoreException(String.format("No active transaction. [thread id=%d][data store=%s]", Thread.currentThread().getId(), dataStore.name()));
                }
                ((TransactionDataStore) dataStore).rollback();
            }
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public void closeStores() throws DataStoreException {
        dataStoreManager.closeStores();
    }

    public <T, E extends IEntity> E create(@Nonnull E entity,
                                           @Nonnull Class<? extends E> type,
                                           @Nonnull String dataStoreName,
                                           @Nonnull Class<? extends AbstractDataStore<T>> storeType,
                                           @Nonnull Principal user,
                                           Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = dataStoreManager.getDataStore(dataStoreName, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]",
                    type.getCanonicalName()));
        }
        return create(entity, type, dataStore, user, context);
    }

    public <T, E extends IEntity> E create(@Nonnull E entity,
                                           @Nonnull Class<? extends E> type,
                                           @Nonnull Principal user,
                                           Context context,
                                           Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {
        Object shardKey = null;
        if (entity instanceof IShardedEntity) {
            shardKey = ((IShardedEntity) entity).getShardKey();
        }
        AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]",
                    type.getCanonicalName()));
        }

        return create(entity, type, dataStore, user, context);
    }

    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> E create(@Nonnull E entity,
                                           @Nonnull Class<? extends E> type,
                                           @Nonnull AbstractDataStore<T> dataStore,
                                           @Nonnull Principal user,
                                           Context context) throws DataStoreException {
        try {
            beingTransaction(dataStore);
            entity.validate();
            entity = (E) formatEntity(entity, context);
            entity.validate();
            entity = dataStore.create(entity, type, context);
            auditChange(dataStore, EAuditType.Create, entity, entity.getClass(), context, null, user);

            return createReferences(entity, type, user, context);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private <E extends IEntity> E createReferences(E entity,
                                                   Class<? extends E> entityType,
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
                                Object t = create((IEntity) v, (Class<? extends IEntity>) type, user, context, null);
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
                                Object t = create((IEntity) v, (Class<? extends IEntity>) type, user, context, null);
                                if (t == null) {
                                    throw new DataStoreException(
                                            String.format("Error creating nested entity. [type=%s][key=%s]",
                                                    type.getCanonicalName(), ((IEntity) v).getKey().stringKey()));
                                }
                            }
                        } else {
                            Object t = create((IEntity) value, (Class<? extends IEntity>) type, user, context, null);
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
            if (source != null) {
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
            }
            return fields;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <T, E extends IEntity> E update(@Nonnull E entity,
                                           @Nonnull Class<? extends E> type,
                                           @Nonnull Principal user,
                                           Context context,
                                           Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {

        Object shardKey = null;
        if (entity instanceof IShardedEntity) {
            shardKey = ((IShardedEntity) entity).getShardKey();
        }
        AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        return update(entity, type, dataStore, user, context);
    }

    public <T, E extends IEntity> E update(@Nonnull E entity,
                                           @Nonnull Class<? extends E> type,
                                           @Nonnull String dataStoreName,
                                           @Nonnull Class<? extends AbstractDataStore<T>> storeType,
                                           @Nonnull Principal user,
                                           Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = dataStoreManager.getDataStore(dataStoreName, storeType);

        return update(entity, type, dataStore, user, context);
    }

    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> E update(@Nonnull E entity,
                                           @Nonnull Class<? extends E> type,
                                           @Nonnull AbstractDataStore<T> dataStore,
                                           @Nonnull Principal user,
                                           Context context) throws DataStoreException {
        try {
            beingTransaction(dataStore);
            entity.validate();
            E prev = null;
            if (type.isAnnotationPresent(Audited.class)) {
                prev = (E) find(entity.getKey(), type, dataStore, context);
                if (prev == null) {
                    throw new DataStoreException(String.format("Current entity record not found. [type=%s][key=%s]",
                            type.getCanonicalName(), entity.getKey().stringKey()));
                }
            }
            entity = (E) formatEntity(entity, context);
            entity.validate();
            entity = dataStore.update(entity, type, context);
            String delta = null;
            if (entity instanceof IChange) {
                JsonNode node = ((IChange) entity).getChange(prev);
                if (node != null) {
                    delta = node.toPrettyString();
                }
            }
            auditChange(dataStore, EAuditType.Update, entity, entity.getClass(), context, delta, user);
            return updateReferences(entity, type, user, context);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private <E extends IEntity> E updateReferences(E entity,
                                                   Class<? extends E> entityType,
                                                   Principal user,
                                                   Context context) throws DataStoreException {
        try {
            List<Field> fields = getReferenceFields(entityType);
            if (fields != null && !fields.isEmpty()) {
                for (Field f : fields) {
                    Reference reference = f.getAnnotation(Reference.class);
                    if (reference.type() != EJoinType.One2Many && reference.type() != EJoinType.One2Many) continue;

                    Object value = ReflectionUtils.getFieldValue(entity, f);
                    if (value != null) {
                        Class<?> type = f.getType();
                        if (ReflectionUtils.implementsInterface(List.class, type) | ReflectionUtils.implementsInterface(Set.class, type)) {
                            type = ReflectionUtils.getGenericListType(f);
                            Collection values = (Collection) value;
                            BaseSearchResult<? extends IEntity> result = getReferenceEntity(f, entity, entityType, (Class<? extends IEntity>) type, context, false);
                            Map<String, Object> rmap = new HashMap<>();
                            if (result instanceof EntitySearchResult) {
                                if (((EntitySearchResult<? extends IEntity>) result).getEntities() != null
                                        && !((EntitySearchResult<? extends IEntity>) result).getEntities().isEmpty()) {
                                    for (Object o : ((EntitySearchResult<? extends IEntity>) result).getEntities()) {
                                        IEntity e = (IEntity) o;
                                        rmap.put(e.getKey().stringKey(), e);
                                    }
                                }
                            }
                            for (Object v : values) {
                                IEntity e = (IEntity) v;
                                String sk = e.getKey().stringKey();
                                if (!rmap.containsKey(sk)) {
                                    Object t = create((IEntity) v, (Class<? extends IEntity>) type, user, context, null);
                                    if (t == null) {
                                        throw new DataStoreException(
                                                String.format("Error creating nested entity. [type=%s][key=%s]",
                                                        type.getCanonicalName(), ((IEntity) v).getKey().stringKey()));
                                    }
                                } else {
                                    Object t = update((IEntity) v, (Class<? extends IEntity>) type, user, context, null);
                                    if (t == null) {
                                        throw new DataStoreException(
                                                String.format("Error creating nested entity. [type=%s][key=%s]",
                                                        type.getCanonicalName(), ((IEntity) v).getKey().stringKey()));
                                    }
                                }
                            }
                            if (!rmap.isEmpty()) {
                                List<IEntity> removed = new ArrayList<>();
                                Map<String, Object> smap = new HashMap<>();
                                for (Object o : values) {
                                    IEntity e = (IEntity) o;
                                    smap.put(e.getKey().stringKey(), e);
                                }
                                for (String k : rmap.keySet()) {
                                    if (!smap.containsKey(k)) {
                                        removed.add((IEntity) rmap.get(k));
                                    }
                                }
                                if (!removed.isEmpty()) {
                                    if (reference.type() == EJoinType.One2Many) {
                                        for (IEntity e : removed) {
                                            if (!delete(e, e.getClass(), user, context, null)) {
                                                throw new DataStoreException(String.format("Error deleting reference. [type=%s][key=%s]",
                                                        e.getClass().getCanonicalName(), e.getKey().stringKey()));
                                            }
                                            LogUtils.debug(getClass(), String.format("Deleted entity reference. [type=%s][key=%s]",
                                                    e.getClass().getCanonicalName(), e.getKey().stringKey()));
                                        }
                                    }
                                }
                            }
                        } else {
                            BaseSearchResult<? extends IEntity> result = getReferenceEntity(f, entity, entityType, (Class<? extends IEntity>) f.getType(), context, false);
                            if (result instanceof EntitySearchResult) {
                                if (((EntitySearchResult<? extends IEntity>) result).getEntities() == null
                                        || ((EntitySearchResult<? extends IEntity>) result).getEntities().isEmpty()) {
                                    Object t = create((IEntity) value, (Class<? extends IEntity>) type, null, user, context);
                                    if (t == null) {
                                        throw new DataStoreException(
                                                String.format("Error creating nested entity. [type=%s][key=%s]",
                                                        type.getCanonicalName(), ((IEntity) value).getKey().stringKey()));
                                    }
                                } else {
                                    while (((EntitySearchResult<? extends IEntity>) result).getEntities().iterator().hasNext()) {
                                        IEntity e = ((EntitySearchResult<? extends IEntity>) result).getEntities().iterator().next();
                                        IEntity v = (IEntity) value;
                                        if (e.getKey().compareTo(v.getKey()) != 0) {
                                            if (!delete(e, e.getClass(), null, user, context)) {
                                                throw new DataStoreException(String.format("Error deleting reference. [type=%s][key=%s]",
                                                        e.getClass().getCanonicalName(), e.getKey().stringKey()));
                                            }
                                            Object t = create(v, v.getClass(), null, user, context);
                                            if (t == null) {
                                                throw new DataStoreException(
                                                        String.format("Error creating nested entity. [type=%s][key=%s]",
                                                                type.getCanonicalName(), ((IEntity) value).getKey().stringKey()));
                                            }
                                        } else {
                                            Object t = update(v, v.getClass(), null, user, context);
                                            if (t == null) {
                                                throw new DataStoreException(
                                                        String.format("Error creating nested entity. [type=%s][key=%s]",
                                                                type.getCanonicalName(), ((IEntity) value).getKey().stringKey()));
                                            }
                                        }
                                    }
                                }
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
                                                                    @Nonnull String dataStoreName,
                                                                    @Nonnull Class<? extends AbstractDataStore<T>> storeType,
                                                                    @Nonnull Principal user,
                                                                    Context context) throws DataStoreException {
        AbstractDataStore<T> dataStore = dataStoreManager.getDataStore(dataStoreName, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        return delete(entity, type, dataStore, user, context);
    }

    public <T, K extends IKey, E extends IEntity<K>> boolean delete(@Nonnull E entity,
                                                                    @Nonnull Class<? extends E> type,
                                                                    @Nonnull Principal user,
                                                                    Context context,
                                                                    Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {
        Object shardKey = null;
        if (entity instanceof IShardedEntity) {
            shardKey = ((IShardedEntity) entity).getShardKey();
        }
        AbstractDataStore<T> dataStore = findStore(type, storeType, shardKey);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        return delete(entity, type, dataStore, user, context);
    }

    public <T, K extends IKey, E extends IEntity<K>> boolean delete(@Nonnull E entity,
                                                                    @Nonnull Class<? extends E> type,
                                                                    @Nonnull AbstractDataStore<T> dataStore,
                                                                    @Nonnull Principal user,
                                                                    Context context) throws DataStoreException {
        try {
            beingTransaction(dataStore);
            E prev = null;
            if (type.isAnnotationPresent(Audited.class)) {
                prev = (E) find(entity.getKey(), type, dataStore, context);
                if (prev == null) {
                    throw new DataStoreException(String.format("Current entity record not found. [type=%s][key=%s]",
                            type.getCanonicalName(), entity.getKey().stringKey()));
                }
            }
            entity = (E) formatEntity(entity, context);
            deleteReferences(entity, type, user, context);

            boolean ret = dataStore.delete(entity.getKey(), type, context);
            if (ret) {
                auditChange(dataStore, EAuditType.Delete, entity, entity.getClass(), context, null, user);
            }
            return ret;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private <E extends IEntity> E deleteReferences(E entity,
                                                   Class<? extends E> entityType,
                                                   Principal user,
                                                   Context context) throws DataStoreException {
        try {
            List<Field> fields = getReferenceFields(entityType);
            if (fields != null && !fields.isEmpty()) {
                for (Field f : fields) {
                    Reference reference = f.getAnnotation(Reference.class);
                    if (reference.type() != EJoinType.One2Many && reference.type() != EJoinType.One2Many) continue;

                    Object value = ReflectionUtils.getFieldValue(entity, f);
                    if (value != null) {
                        Class<?> type = f.getType();
                        if (ReflectionUtils.implementsInterface(List.class, type) | ReflectionUtils.implementsInterface(Set.class, type)) {
                            type = ReflectionUtils.getGenericListType(f);
                            BaseSearchResult<? extends IEntity> result
                                    = getReferenceEntity(f, entity, entityType, (Class<? extends IEntity>) type, context, false);
                            if (result instanceof EntitySearchResult) {
                                for (IEntity v : ((EntitySearchResult<? extends IEntity>) result).getEntities()) {
                                    String sk = v.getKey().stringKey();
                                    if (!delete(v, v.getClass(), user, context, null)) {
                                        throw new DataStoreException(String.format("Error deleting reference. [type=%s][key=%s]",
                                                v.getClass().getCanonicalName(), v.getKey().stringKey()));
                                    }
                                    LogUtils.debug(getClass(), String.format("Deleted entity reference. [type=%s][key=%s]",
                                            v.getClass().getCanonicalName(), v.getKey().stringKey()));
                                }
                            }
                        } else {
                            BaseSearchResult<? extends IEntity> result
                                    = getReferenceEntity(f, entity, entityType, (Class<? extends IEntity>) f.getType(), context, false);
                            if (result instanceof EntitySearchResult) {
                                while (((EntitySearchResult<? extends IEntity>) result).getEntities().iterator().hasNext()) {
                                    IEntity e = ((EntitySearchResult<? extends IEntity>) result).getEntities().iterator().next();
                                    if (!delete(e, e.getClass(), user, context, null)) {
                                        throw new DataStoreException(String.format("Error deleting reference. [type=%s][key=%s]",
                                                e.getClass().getCanonicalName(), e.getKey().stringKey()));
                                    }
                                    LogUtils.debug(getClass(), String.format("Deleted entity reference. [type=%s][key=%s]",
                                            e.getClass().getCanonicalName(), e.getKey().stringKey()));
                                }
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

    private <K extends IKey, E extends IEntity<K>> void auditChange(AbstractDataStore dataStore,
                                                                    EAuditType auditType,
                                                                    E entity,
                                                                    Class<? extends E> entityType,
                                                                    Context context,
                                                                    String changeDelta,
                                                                    Principal user) throws DataStoreException {
        try {
            if (dataStore.config().audited() || entityType.isAnnotationPresent(Audited.class)) {
                String changeContext = null;
                if (dataStore.config().auditContextProvider() != null) {
                    IAuditContextGenerator provider = AuditManager.get().getContextGenerator(dataStore.config().auditContextProvider());
                    if (provider == null) {
                        throw new DataStoreException(String.format("Audit Context generator not found. [type=%s]",
                                dataStore.config().auditContextProvider().getCanonicalName()));
                    }
                    AbstractAuditContext ctx = provider.generate(dataStore, entity, context, user);
                    if (ctx != null) {
                        changeContext = ctx.json();
                    }
                }
                if (dataStore.auditLogger() == null) {
                    String logger = dataStore.config().auditLogger();
                    AbstractAuditLogger auditLogger = AuditManager.get().getLogger(logger);
                    if (auditLogger == null) {
                        throw new DataStoreException(String.format("Error getting audit logger instance. [data store=%s:%s][entity type=%s]",
                                dataStore.getClass().getCanonicalName(), dataStore.name(), entityType.getCanonicalName()));
                    }
                    dataStore.auditLogger(auditLogger);
                }
                AuditRecord r = dataStore.auditLogger().write(dataStore.getClass(), dataStore.name(), auditType, entity, entityType, changeDelta, changeContext, user);
                if (r == null) {
                    throw new DataStoreException(String.format("Error creating audit record. [data store=%s:%s][entity type=%s]",
                            dataStore.getClass().getCanonicalName(), dataStore.name(), entityType.getCanonicalName()));
                }
            }
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
        return find(key, type, dataStore, context);
    }

    public <T, E extends IEntity> E find(@Nonnull Object key,
                                         @Nonnull Class<? extends E> type,
                                         @Nonnull AbstractDataStore<T> dataStore,
                                         Context context) throws DataStoreException {
        E value = dataStore.find(key, type, context);
        if (value != null)
            findReferences(value, type, context);
        return value;
    }

    public <T, E extends IEntity> BaseSearchResult<E> search(@Nonnull String query,
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
        return search(query, type, dataStore, context);
    }

    public <T, E extends IEntity> BaseSearchResult<E> search(@Nonnull String query,
                                                             @Nonnull Class<? extends E> type,
                                                             @Nonnull AbstractDataStore<T> dataStore,
                                                             Context context) throws DataStoreException {
        BaseSearchResult<E> values = dataStore.search(query, type, context);
        if (values instanceof EntitySearchResult) {
            if (!((EntitySearchResult<E>) values).getEntities().isEmpty())
                return findReferences(((EntitySearchResult<E>) values).getEntities(), type, context);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> BaseSearchResult<E> search(Object shardKey,
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
            BaseSearchResult<E> values = dataStore.search(query, type, context);
            if (values instanceof EntitySearchResult) {
                if (!((EntitySearchResult<E>) values).getEntities().isEmpty())
                    return findReferences(((EntitySearchResult<E>) values).getEntities(), type, context);
            }
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

    public <T, E extends IEntity> BaseSearchResult<E> search(@Nonnull String query,
                                                             int offset,
                                                             int maxResults,
                                                             @Nonnull Class<? extends E> type,
                                                             Context context,
                                                             Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {
        Preconditions.checkArgument(!type.isAnnotationPresent(SchemaSharded.class));
        AbstractDataStore<T> dataStore = findStore(type, storeType);
        if (dataStore == null) {
            throw new DataStoreException(String.format("No data store found for entity. [type=%s]", type.getCanonicalName()));
        }
        if (ReflectionUtils.implementsInterface(IShardedEntity.class, type)) {
            throw new DataStoreException(String.format("Sharded entity should be called with shard key. [type=%s]", type.getCanonicalName()));
        }
        return search(query, offset, maxResults, type, dataStore, context);
    }

    public <T, E extends IEntity> BaseSearchResult<E> search(@Nonnull String query,
                                                             int offset,
                                                             int maxResults,
                                                             @Nonnull Class<? extends E> type,
                                                             @Nonnull AbstractDataStore<T> dataStore,
                                                             Context context) throws DataStoreException {
        BaseSearchResult<E> values = dataStore.search(query, offset, maxResults, type, context);
        if (values instanceof EntitySearchResult) {
            if (!((EntitySearchResult<E>) values).getEntities().isEmpty())
                return findReferences(((EntitySearchResult<E>) values).getEntities(), type, context);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> BaseSearchResult<E> search(Object shardKey,
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
            BaseSearchResult<E> values = dataStore.search(query, offset, maxResults, type, context);
            if (values instanceof EntitySearchResult) {
                if (!((EntitySearchResult<E>) values).getEntities().isEmpty())
                    return findReferences(((EntitySearchResult<E>) values).getEntities(), type, context);
            }
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

    public <T, E extends IEntity> BaseSearchResult<E> search(@Nonnull String query,
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
        BaseSearchResult<E> values = dataStore.search(query, params, type, context);
        if (values instanceof EntitySearchResult) {
            if (!((EntitySearchResult<E>) values).getEntities().isEmpty())
                return findReferences(((EntitySearchResult<E>) values).getEntities(), type, context);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> BaseSearchResult<E> search(Object shardKey,
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
            BaseSearchResult<E> values = dataStore.search(query, params, type, context);
            if (values instanceof EntitySearchResult) {
                if (!((EntitySearchResult<E>) values).getEntities().isEmpty())
                    return findReferences(((EntitySearchResult<E>) values).getEntities(), type, context);
            }
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

    public <T, E extends IEntity> BaseSearchResult<E> search(@Nonnull String query,
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
        BaseSearchResult<E> values = dataStore.search(query, offset, maxResults, params, type, context);
        if (values instanceof EntitySearchResult) {
            if (!((EntitySearchResult<E>) values).getEntities().isEmpty())
                return findReferences(((EntitySearchResult<E>) values).getEntities(), type, context);
        }
        return null;
    }


    @SuppressWarnings("unchecked")
    public <T, E extends IEntity> BaseSearchResult<E> search(Object shardKey,
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
            BaseSearchResult<E> values = dataStore.search(query, offset, maxResults, params, type, context);
            if (values instanceof EntitySearchResult) {
                if (!((EntitySearchResult<E>) values).getEntities().isEmpty())
                    return findReferences(((EntitySearchResult<E>) values).getEntities(), type, context);
            }
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
    private <E extends IEntity> BaseSearchResult<E> findReferences(Collection<E> entities,
                                                                   @Nonnull Class<? extends E> entityType,
                                                                   Context context) throws DataStoreException {
        try {
            List<Field> fields = getReferenceFields(entityType);
            if (fields != null && !fields.isEmpty()) {
                for (Field f : fields) {
                    Reference reference = f.getAnnotation(Reference.class);
                    String query = JoinPredicateHelper.generateHibernateJoinQuery(reference, entities, f, dataStoreManager, true);
                    if (Strings.isNullOrEmpty(query)) {
                        throw new DataStoreException(String.format("NULL query returned. [type=%s][field=%s]",
                                entityType.getCanonicalName(), f.getName()));
                    }
                    int offset = 0;
                    Multimap<String, E> parentMap = (Multimap<String, E>) mapCollection(entities, reference, true);
                    while (true) {
                        BaseSearchResult result = search(query,
                                offset,
                                DEFAULT_BATCH_SIZE,
                                (Class<? extends IEntity>) f.getType(),
                                null, context);
                        if (result instanceof EntitySearchResult) {
                            if (((EntitySearchResult) result).getEntities() != null && !((EntitySearchResult) result).getEntities().isEmpty()) {
                                joinResults(parentMap, ((EntitySearchResult) result).getEntities(), f, entityType, reference);
                            }
                            if (((EntitySearchResult) result).getEntities() == null || ((EntitySearchResult) result).getEntities().size() < DEFAULT_BATCH_SIZE)
                                break;
                            offset += ((EntitySearchResult) result).getEntities().size();
                        }
                    }
                }
            }
            EntitySearchResult<E> er = new EntitySearchResult<>(entityType);
            er.setCount(entities.size());
            er.setEntities(entities);

            return er;
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
                    Class<?> type = f.getType();
                    Class<?> itype = type;
                    if (ReflectionUtils.implementsInterface(List.class, type)) {
                        itype = ReflectionUtils.getGenericListType(f);
                    } else if (ReflectionUtils.implementsInterface(Set.class, type)) {
                        itype = ReflectionUtils.getGenericSetType(f);
                    }

                    BaseSearchResult result = getReferenceEntity(f, entity,
                            entityType, (Class<? extends IEntity>) itype,
                            context, true);
                    if (result instanceof EntitySearchResult) {
                        if (((EntitySearchResult) result).getEntities() != null && !((EntitySearchResult) result).getEntities().isEmpty()) {
                            if (ReflectionUtils.implementsInterface(List.class, type)) {
                                List values = new ArrayList(((EntitySearchResult) result).getEntities());
                                ReflectionUtils.setObjectValue(entity, f, values);
                            } else if (ReflectionUtils.implementsInterface(Set.class, type)) {
                                Set values = new HashSet(((EntitySearchResult) result).getEntities());
                                ReflectionUtils.setObjectValue(entity, f, values);
                            } else {
                                while (((EntitySearchResult) result).getEntities().iterator().hasNext()) {
                                    Object o = ((EntitySearchResult) result).getEntities().iterator().next();
                                    ReflectionUtils.setObjectValue(entity, f, o);
                                    break;
                                }
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

    private <E extends IEntity> BaseSearchResult<? extends IEntity> getReferenceEntity(Field f,
                                                                                       E entity,
                                                                                       Class<? extends E> entityType,
                                                                                       Class<? extends IEntity> fieldType,
                                                                                       Context context,
                                                                                       boolean appendQuery) throws DataStoreException {
        Reference reference = f.getAnnotation(Reference.class);
        String query = JoinPredicateHelper.generateHibernateJoinQuery(reference, entity, f, dataStoreManager, appendQuery);
        if (Strings.isNullOrEmpty(query)) {
            throw new DataStoreException(String.format("NULL query returned. [type=%s][field=%s]",
                    entityType.getCanonicalName(), f.getName()));
        }
        int offset = 0;
        List<E> entities = new ArrayList<>();
        while (true) {
            BaseSearchResult result = search(query,
                    offset,
                    DEFAULT_BATCH_SIZE,
                    fieldType,
                    context, null);
            if (result instanceof EntitySearchResult) {
                if (((EntitySearchResult) result).getEntities() != null
                        && !((EntitySearchResult) result).getEntities().isEmpty()) {
                    entities.addAll(((EntitySearchResult) result).getEntities());
                }
                if (((EntitySearchResult) result).getEntities() == null
                        || ((EntitySearchResult) result).getEntities().size() < DEFAULT_BATCH_SIZE) break;
                offset += ((EntitySearchResult) result).getEntities().size();
            }
        }
        EntitySearchResult<E> er = new EntitySearchResult<>(entityType);
        er.setCount(entities.size());
        er.setEntities(entities);

        return er;
    }

    public void close() throws IOException {
        try {
            if (dataStoreManager != null)
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
