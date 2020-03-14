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
import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.*;
import com.codekutter.common.stores.impl.DataStoreAuditContext;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.lucene.search.Query;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class ElasticSearchDataStore extends AbstractDataStore<RestHighLevelClient> implements ISearchable {
    private final ElasticSearchHelper helper = new ElasticSearchHelper();

    @Override
    public void configureDataStore(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        Preconditions.checkState(config() instanceof ElasticSearchConfig);
        try {
            AbstractConnection<RestHighLevelClient> connection =
                    dataStoreManager.getConnection(config().connectionName(), RestHighLevelClient.class);
            if (!(connection instanceof ElasticSearchConnection)) {
                throw new ConfigurationException(String.format("No connection found for name. [name=%s]", config().connectionName()));
            }
            withConnection(connection);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public <E extends IEntity> E createEntity(@Nonnull E entity,
                                              @Nonnull Class<? extends E> type,
                                              Context context) throws DataStoreException {
        try {
            return helper.createEntity(connection().connection(), entity, type, context);
        } catch (ConnectionException ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity> E updateEntity(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        try {
            return helper.updateEntity(connection().connection(), entity, type, context);
        } catch (ConnectionException t) {
            throw new DataStoreException(t);
        }
    }

    @Override
    public <E extends IEntity> boolean deleteEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        try {
            return helper.deleteEntity(connection().connection(), key, type, context);
        } catch (ConnectionException t) {
            throw new DataStoreException(t);
        }
    }

    @Override
    public <E extends IEntity> E findEntity(@Nonnull Object key,
                                            @Nonnull Class<? extends E> type,
                                            Context context) throws DataStoreException {
        try {
            return helper.findEntity(connection().connection(), key, type, context);
        } catch (ConnectionException t) {
            throw new DataStoreException(t);
        }
    }

    @Override
    public <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query, int offset, int maxResults,
                                                            @Nonnull Class<? extends E> type,
                                                            Context context) throws DataStoreException {
        return textSearch(query, maxResults, offset, type, context);
    }

    @Override
    public <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query, int offset, int maxResults,
                                                            Map<String, Object> parameters,
                                                            @Nonnull Class<? extends E> type,
                                                            Context context) throws DataStoreException {
        return textSearch(query, maxResults, offset, type, context);
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
    public void close() throws IOException {
        super.close();
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull Query query,
                                                              @Nonnull Class<? extends T> type,
                                                              Context context) throws DataStoreException {
        return textSearch(query, maxResults(), 0, type, context);
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull Query query,
                                                              int batchSize,
                                                              int offset,
                                                              @Nonnull Class<? extends T> type,
                                                              Context context) throws DataStoreException {
        String q = query.toString();
        return textSearch(q, batchSize, offset, type, context);
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull String query,
                                                              @Nonnull Class<? extends T> type,
                                                              Context context) throws DataStoreException {
        return textSearch(query, maxResults(), 0, type, context);
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull String query,
                                                              int batchSize,
                                                              int offset,
                                                              @Nonnull Class<? extends T> type,
                                                              Context context) throws DataStoreException {
        try {
            return helper.textSearch(connection().connection(), query, batchSize, offset, type, context);
        } catch (ConnectionException ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <T extends IEntity> BaseSearchResult<T> facetedSearch(@Nonnull Object query, @Nonnull Class<? extends T> type, Context context) throws DataStoreException {
        String index = helper.getIndexName(type);
        if (Strings.isNullOrEmpty(index)) {
            throw new DataStoreException(String.format("Type is not indexed. [type=%s]", type.getCanonicalName()));
        }
        try {
            List<SortBuilder<?>> sortBuilders = null;
            if (context instanceof ElasticSearchContext) {
                sortBuilders = ((ElasticSearchContext) context).sort();
            }
            if (query instanceof AbstractAggregationBuilder) {
                return helper.facetedSearch((AbstractAggregationBuilder) query, index, connection().connection(), type, sortBuilders);
            } else if (query instanceof AbstractAggregationBuilder[]) {
                return helper.facetedSearch((AbstractAggregationBuilder[]) query, index, connection().connection(), type, sortBuilders);
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
        throw new DataStoreException(String.format("Query type not supported. [type=%s]", query.getClass().getCanonicalName()));
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
        try {
            return helper.textSearch(connection().connection(), query, batchSize, offset, type, context);
        } catch (ConnectionException ex) {
            throw new DataStoreException(ex);
        }
    }
}
