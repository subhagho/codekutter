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
import com.codekutter.common.GlobalConstants;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.IKey;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.r2db.driver.impl.annotations.Indexed;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class ElasticSearchHelper {
    public <E extends IEntity> E createEntity(@Nonnull RestHighLevelClient client,
                                              @Nonnull E entity,
                                              @Nonnull Class<? extends IEntity> type,
                                              Context context) throws DataStoreException {
        try {
            String index = type.getCanonicalName();
            if (type.isAnnotationPresent(Indexed.class)) {
                Indexed indx = type.getAnnotation(Indexed.class);
                if (!Strings.isNullOrEmpty(indx.index())) {
                    index = indx.index();
                }
            }
            String json = GlobalConstants.getJsonMapper().writeValueAsString(entity);
            IndexRequest request = new IndexRequest(index);
            request.id(entity.getKey().stringKey());
            request.source(XContentType.JSON, json);
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                LogUtils.debug(getClass(), json);
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                LogUtils.debug(getClass(), json);
            }
            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                throw new DataStoreException(String.format("Error replicating to shards. [count=%d][index=%s]", shardInfo.getSuccessful(), index));
            }
            if (shardInfo.getFailed() > 0) {
                StringBuffer buffer = new StringBuffer();
                for (ReplicationResponse.ShardInfo.Failure failure :
                        shardInfo.getFailures()) {
                    String reason = failure.reason();
                    buffer.append(String.format("[%s] Shard failed : %s\n", index, reason));
                }
                throw new DataStoreException(buffer.toString());
            }
            return entity;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public <E extends IEntity> E updateEntity(@Nonnull RestHighLevelClient client,
                                              @Nonnull E entity,
                                              @Nonnull Class<? extends IEntity> type,
                                              Context context) throws DataStoreException {
        try {
            String index = type.getCanonicalName();
            if (type.isAnnotationPresent(Indexed.class)) {
                Indexed indx = type.getAnnotation(Indexed.class);
                if (!Strings.isNullOrEmpty(indx.index())) {
                    index = indx.index();
                }
            }
            String json = GlobalConstants.getJsonMapper().writeValueAsString(entity);
            UpdateRequest request = new UpdateRequest(index, entity.getKey().stringKey());
            request.doc(XContentType.JSON, json);
            UpdateResponse response = client.update(request,RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                LogUtils.debug(getClass(), json);
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                LogUtils.debug(getClass(), json);
            }
            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                throw new DataStoreException(String.format("Error replicating to shards. [count=%d][index=%s]", shardInfo.getSuccessful(), index));
            }
            if (shardInfo.getFailed() > 0) {
                StringBuffer buffer = new StringBuffer();
                for (ReplicationResponse.ShardInfo.Failure failure :
                        shardInfo.getFailures()) {
                    String reason = failure.reason();
                    buffer.append(String.format("[%s] Shard failed : %s\n", index, reason));
                }
                throw new DataStoreException(buffer.toString());
            }
            return entity;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public <E extends IEntity> boolean deleteEntity(@Nonnull RestHighLevelClient client,
                                                    @Nonnull Object key,
                                                    @Nonnull Class<? extends E> type,
                                                    Context context) throws DataStoreException {
        Preconditions.checkArgument(key instanceof IKey);
        IKey k = (IKey) key;
        try {
            String index = type.getCanonicalName();
            if (type.isAnnotationPresent(Indexed.class)) {
                Indexed indx = type.getAnnotation(Indexed.class);
                if (!Strings.isNullOrEmpty(indx.index())) {
                    index = indx.index();
                }
            }
            DeleteRequest request = new DeleteRequest(index,  k.stringKey());
            DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.DELETED) {
                return true;
            }
            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                throw new DataStoreException(String.format("Error replicating to shards. [count=%d][index=%s]", shardInfo.getSuccessful(), index));
            }
            if (shardInfo.getFailed() > 0) {
                StringBuffer buffer = new StringBuffer();
                for (ReplicationResponse.ShardInfo.Failure failure :
                        shardInfo.getFailures()) {
                    String reason = failure.reason();
                    buffer.append(String.format("[%s] Shard failed : %s\n", index, reason));
                }
                throw new DataStoreException(buffer.toString());
            }
            return false;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public <E extends IEntity> E findEntity(@Nonnull RestHighLevelClient client,
                                            @Nonnull Object key,
                                            @Nonnull Class<? extends E> type,
                                            Context context) throws DataStoreException {
        Preconditions.checkArgument(key instanceof IKey);
        IKey k = (IKey) key;
        try {
            String index = type.getCanonicalName();
            if (type.isAnnotationPresent(Indexed.class)) {
                Indexed indx = type.getAnnotation(Indexed.class);
                if (!Strings.isNullOrEmpty(indx.index())) {
                    index = indx.index();
                }
            }
            GetRequest request = new GetRequest(index, k.stringKey());
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            if (response.isExists()) {
                String json = response.getSourceAsString();
                E entity = GlobalConstants.getJsonMapper().readValue(json, type);
                LogUtils.debug(getClass(), entity);

                return entity;
            }
            return null;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }


    private <T> List<T> scroll(RestHighLevelClient client,
                               String index,
                               @Nonnull String query,
                               int batchSize,
                               int offset,
                               @Nonnull Class<? extends T> type,
                               ElasticSearchContext context) throws DataStoreException {
        try {
            String scrollId = context.scrollId();
            SearchResponse response = null;
            if (Strings.isNullOrEmpty(scrollId)) {
                SearchRequest request = new SearchRequest(index);
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                sourceBuilder.from(offset);
                sourceBuilder.size(batchSize);
                sourceBuilder.query(QueryBuilders.queryStringQuery(query));
                request.source(sourceBuilder);

                response = client.search(request, RequestOptions.DEFAULT);
            } else {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            }
            if (response.status() != RestStatus.OK && response.status() != RestStatus.NOT_FOUND && response.status() != RestStatus.FOUND) {
                throw new DataStoreException(String.format("Search failed. [status=%s][index=%s]", response.status().name(), index));
            }
            if (response.status() == RestStatus.FOUND) {
                SearchHits hits = response.getHits();
                if (hits != null) {
                    context.scrollId(response.getScrollId());
                    List<T> entities = new ArrayList<T>();
                    SearchHit[] results = hits.getHits();
                    for (SearchHit hit : results) {
                        String json = hit.getSourceAsString();
                        T entity = GlobalConstants.getJsonMapper().readValue(json, type);
                        entities.add(entity);
                    }
                    return entities;
                }
            }
            return null;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public <T> List<T> textSearch(@Nonnull RestHighLevelClient client,
                                  @Nonnull String query,
                                  int batchSize,
                                  int offset,
                                  @Nonnull Class<? extends T> type,
                                  Context context) throws DataStoreException {
        String index = type.getCanonicalName();
        if (type.isAnnotationPresent(Indexed.class)) {
            Indexed indx = type.getAnnotation(Indexed.class);
            if (!Strings.isNullOrEmpty(indx.index())) {
                index = indx.index();
            }
        }
        boolean scroll = false;
        if (context instanceof ElasticSearchContext) {
            scroll = ((ElasticSearchContext) context).doScroll();
        }
        if (scroll) {
            return scroll(client, index, query, batchSize, offset, type, (ElasticSearchContext) context);
        } else {
            return _search(client, index, query, batchSize, offset, type, context);
        }
    }

    private <T> List<T> _search(RestHighLevelClient client,
                                String index,
                                @Nonnull String query,
                                int batchSize,
                                int offset,
                                @Nonnull Class<? extends T> type,
                                Context context) throws DataStoreException {
        try {
            SearchRequest request = new SearchRequest(index);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.from(offset);
            sourceBuilder.size(batchSize);
            sourceBuilder.query(QueryBuilders.queryStringQuery(query));
            request.source(sourceBuilder);

            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            if (response.status() != RestStatus.OK && response.status() != RestStatus.NOT_FOUND && response.status() != RestStatus.FOUND) {
                throw new DataStoreException(String.format("Search failed. [status=%s][index=%s]", response.status().name(), index));
            }
            if (response.status() == RestStatus.FOUND) {
                SearchHits hits = response.getHits();
                if (hits != null) {
                    List<T> entities = new ArrayList<T>();
                    SearchHit[] results = hits.getHits();
                    for (SearchHit hit : results) {
                        String json = hit.getSourceAsString();
                        T entity = GlobalConstants.getJsonMapper().readValue(json, type);
                        entities.add(entity);
                    }
                    return entities;
                }
            }
            return null;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }
}
