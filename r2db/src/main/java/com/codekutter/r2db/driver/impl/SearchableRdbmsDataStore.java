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

package com.codekutter.r2db.driver.impl;

import com.codekutter.common.Context;
import com.codekutter.common.model.DocumentEntity;
import com.codekutter.common.model.EAuditType;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.IKey;
import com.codekutter.common.stores.*;
import com.codekutter.common.stores.impl.HibernateConnection;
import com.codekutter.common.stores.impl.RdbmsConfig;
import com.codekutter.common.stores.impl.RdbmsDataStore;
import com.codekutter.r2db.driver.impl.annotations.Indexed;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.lucene.search.Query;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.hibernate.Session;

import javax.annotation.Nonnull;
import javax.persistence.Entity;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public class SearchableRdbmsDataStore extends RdbmsDataStore implements ISearchable {
    private final ElasticSearchHelper helper = new ElasticSearchHelper();
    private ElasticSearchConnection readConnection = null;
    private Map<Class<? extends IEntity>, Map<IKey, CacheEntry>> dirtyCache = new HashMap<>();

    @Override
    public void commit() throws DataStoreException {
        super.commit();
        if (!dirtyCache.isEmpty()) {
            for (Class<? extends IEntity> type : dirtyCache.keySet()) {
                Map<IKey, CacheEntry> entries = dirtyCache.get(type);
                if (entries != null && !entries.isEmpty()) {
                    for (IKey key : entries.keySet()) {
                        CacheEntry entry = entries.get(key);
                        if (entry.entryType == EAuditType.Create) {
                            helper.createEntity(readConnection.connection(), entry.entity, type, null);
                        } else if (entry.entryType == EAuditType.Update) {
                            helper.updateEntity(readConnection.connection(), entry.entity, type, null);
                        } else if (entry.entryType == EAuditType.Delete) {
                            helper.deleteEntity(readConnection.connection(), key, type, null);
                        }
                    }
                }
            }
        }
        dirtyCache.clear();
    }

    @Override
    public void rollback() throws DataStoreException {
        super.rollback();
        dirtyCache.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity> E findEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        boolean indexedOnly = false;
        if (type.isAnnotationPresent(Indexed.class)) {
            indexedOnly = true;
        }
        if (type.isAnnotationPresent(Entity.class)) {
            indexedOnly = false;
        }
        if (type.isAnnotationPresent(Indexed.class)) {
            if (dirtyCache.containsKey(type)) {
                Map<IKey, CacheEntry> entries = dirtyCache.get(type);
                if (key instanceof IKey) {
                    IKey k = (IKey) key;
                    if (entries.containsKey(k)) {
                        CacheEntry entry = entries.get(k);
                        if (entry.entryType != EAuditType.Delete) {
                            return (E) entry.entity;
                        }
                    }
                }
            }
        }
        if (!indexedOnly)
            return super.findEntity(key, type, context);
        else {
            try {
                return helper.findEntity(readConnection.connection(), key, type, context);
            } catch (Exception ex) {
                throw new DataStoreException(ex);
            }
        }
    }

    @Override
    public <E extends IEntity> E createEntity(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        boolean indexedOnly = false;
        if (type.isAnnotationPresent(Indexed.class)) {
            indexedOnly = true;
        }
        if (type.isAnnotationPresent(Entity.class)) {
            indexedOnly = false;
        }
        if (!indexedOnly)
            entity = super.createEntity(entity, type, context);
        if (type.isAnnotationPresent(Indexed.class)) {
            CacheEntry ce = new CacheEntry();
            ce.entryType = EAuditType.Create;
            ce.entity = entity;
            Map<IKey, CacheEntry> entries = null;
            if (dirtyCache.containsKey(type)) {
                entries = dirtyCache.get(type);
            } else {
                entries = new HashMap<>();
                dirtyCache.put(type, entries);
            }
            entries.put(entity.getKey(), ce);
        }
        return entity;
    }

    @Override
    public <E extends IEntity> E updateEntity(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        boolean indexedOnly = false;
        if (type.isAnnotationPresent(Indexed.class)) {
            indexedOnly = true;
        }
        if (type.isAnnotationPresent(Entity.class)) {
            indexedOnly = false;
        }
        if (!indexedOnly)
            entity = super.updateEntity(entity, type, context);
        if (type.isAnnotationPresent(Indexed.class)) {
            CacheEntry ce = new CacheEntry();
            ce.entryType = EAuditType.Update;
            ce.entity = entity;
            Map<IKey, CacheEntry> entries = null;
            if (dirtyCache.containsKey(type)) {
                entries = dirtyCache.get(type);
            } else {
                entries = new HashMap<>();
                dirtyCache.put(type, entries);
            }
            if (entries.containsKey(entity.getKey())) {
                CacheEntry cce = entries.get(entity.getKey());
                if (cce.entryType == EAuditType.Create) {
                    cce.entity = entity;
                } else if (cce.entryType == EAuditType.Delete) {
                    throw new DataStoreException(String.format("Attempt to update an entity that has been deleted. [type=%s][key=%s]",
                            type.getCanonicalName(), entity.getKey().stringKey()));
                } else {
                    entries.put(entity.getKey(), ce);
                }
            } else
                entries.put(entity.getKey(), ce);
        }
        return entity;
    }

    @Override
    public <E extends IEntity> boolean deleteEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        boolean indexedOnly = false;
        if (type.isAnnotationPresent(Indexed.class)) {
            indexedOnly = true;
        }
        if (type.isAnnotationPresent(Entity.class)) {
            indexedOnly = false;
        }

        boolean ret = true;
        if (!indexedOnly)
            ret = super.deleteEntity(key, type, context);
        if (ret && type.isAnnotationPresent(Indexed.class)) {
            CacheEntry ce = new CacheEntry();
            ce.entryType = EAuditType.Delete;
            ce.entity = null;
            IKey k = (IKey) key;
            Map<IKey, CacheEntry> entries = null;
            if (dirtyCache.containsKey(type)) {
                entries = dirtyCache.get(type);
            } else {
                entries = new HashMap<>();
                dirtyCache.put(type, entries);
            }
            if (entries.containsKey(k)) {
                CacheEntry cce = entries.get(k);
                if (cce.entryType == EAuditType.Create) {
                    entries.remove(k);
                } else if (cce.entryType == EAuditType.Delete) {
                    throw new DataStoreException(String.format("Attempt to delete an entity that has been deleted. [type=%s][key=%s]",
                            type.getCanonicalName(), k.stringKey()));
                } else {
                    entries.put(k, ce);
                }
            } else
                entries.put(k, ce);
        }
        return ret;
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull Query query, @Nonnull Class<? extends T> type, Context context) throws DataStoreException {
        return textSearch(query.toString(), maxResults(), 0, type, context);
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull Query query, int batchSize, int offset, @Nonnull Class<? extends T> type, Context context) throws DataStoreException {
        return textSearch(query.toString(), batchSize, offset, type, context);
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull String query, @Nonnull Class<? extends T> type, Context context) throws DataStoreException {
        return textSearch(query, maxResults(), 0, type, context);
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull String query, int batchSize, int offset, @Nonnull Class<? extends T> type, Context context) throws DataStoreException {
        if (batchSize <= 0) batchSize = maxResults();
        if (offset < 0) offset = 0;
        return helper.textSearch(readConnection.connection(), query, batchSize, offset, type, context);
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> facetedSearch(Object query,
                                                                 @Nonnull Object aggregates,
                                                                 @Nonnull Class<? extends T> type,
                                                                 Context context) throws DataStoreException {
        if (query != null) {
            Preconditions.checkArgument(query instanceof QueryBuilder || query instanceof Query || query instanceof String);
        }
        String index = helper.getIndexName(type);
        if (Strings.isNullOrEmpty(index)) {
            throw new DataStoreException(String.format("Type is not indexed. [type=%s]", type.getCanonicalName()));
        }
        List<SortBuilder<?>> sortBuilders = null;
        if (context instanceof ElasticSearchContext) {
            sortBuilders = ((ElasticSearchContext) context).sort();
        }
        if (aggregates instanceof AbstractAggregationBuilder) {
            if (query instanceof QueryBuilder) {
                return helper.facetedSearch((AbstractAggregationBuilder) aggregates, index, readConnection.connection(), type, (QueryBuilder) query, sortBuilders);
            } else {
                String qstr = null;
                if (query instanceof String) {
                    qstr = (String) query;
                } else if (query instanceof Query) {
                    qstr = ((Query) query).toString();
                }
                return helper.facetedSearch((AbstractAggregationBuilder) aggregates, index, readConnection.connection(), type, qstr, sortBuilders);
            }
        } else if (aggregates instanceof AbstractAggregationBuilder[]) {
            if (query instanceof QueryBuilder) {
                return helper.facetedSearch((AbstractAggregationBuilder[]) aggregates, index, readConnection.connection(), type, (QueryBuilder) query, sortBuilders);
            } else {
                String qstr = null;
                if (query instanceof String) {
                    qstr = (String) query;
                } else if (query instanceof Query) {
                    qstr = ((Query) query).toString();
                }
                return helper.facetedSearch((AbstractAggregationBuilder[]) aggregates, index, readConnection.connection(), type, qstr, sortBuilders);
            }
        }
        throw new DataStoreException(String.format("Query type not supported. [type=%s]", aggregates.getClass().getCanonicalName()));
    }

    @Override
    public void indexDocument(@Nonnull String index,
                              @Nonnull DocumentEntity entity,
                              Context context) throws DataStoreException {
        throw new DataStoreException("Method not implemented...");
    }

    @Override
    public <C> DocumentEntity indexDocument(@Nonnull String index,
                                            @Nonnull String fileDataStore,
                                            @Nonnull Class<? extends AbstractDataStore<C>> dataStoreType,
                                            @Nonnull IKey key,
                                            @Nonnull Class<? extends IEntity> fileEntityType,
                                            Context context) throws DataStoreException {
        throw new DataStoreException("Method not implemented...");
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> searchDocument(@Nonnull String index,
                                                                  @Nonnull String query,
                                                                  int batchSize,
                                                                  int offset,
                                                                  Context context) throws DataStoreException {
        throw new DataStoreException("Method not implemented...");
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> searchDocument(@Nonnull String index,
                                                                  @Nonnull Query query,
                                                                  int batchSize,
                                                                  int offset,
                                                                  @Nonnull Class<? extends T> type,
                                                                  Context context) throws DataStoreException {
        throw new DataStoreException("Method not implemented...");
    }

    public <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull QueryBuilder query,
                                                              @Nonnull Class<? extends T> type,
                                                              Context context) throws DataStoreException {
        return textSearch(query, maxResults(), 0, type, context);
    }

    public <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull QueryBuilder query,
                                                              int batchSize,
                                                              int offset,
                                                              @Nonnull Class<? extends T> type,
                                                              Context context) throws DataStoreException {
        return helper.textSearch(readConnection.connection(), query, batchSize, offset, type, context);
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
            session = connection.connection();
            if (!Strings.isNullOrEmpty(((RdbmsConfig) config()).readConnectionName())) {
                AbstractConnection<RestHighLevelClient> rc =
                        (AbstractConnection<RestHighLevelClient>) dataStoreManager.getConnection(((RdbmsConfig) config()).readConnectionName(), RestHighLevelClient.class);
                if (!(rc instanceof SearchableConnection)) {
                    throw new ConfigurationException(String.format("No connection found for name. [name=%s]", ((RdbmsConfig) config()).readConnectionName()));
                }
                readConnection = (ElasticSearchConnection) rc;
            } else {
                throw new ConfigurationException(String.format("No Search connection specified. [data store=%s]", name()));
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    private static class CacheEntry {
        private EAuditType entryType;
        private IEntity entity;
    }
}
