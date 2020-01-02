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

package com.codekutter.common.stores.impl;

import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.AbstractDataStore;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.DataStoreManager;
import com.codekutter.common.stores.ISearchable;
import com.codekutter.zconfig.common.ConfigurationException;
import org.apache.lucene.search.Query;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ElasticSearchDataStore extends AbstractDataStore<RestHighLevelClient> implements ISearchable {
    @Override
    public void configure(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {

    }

    @Override
    public <E extends IEntity> E create(@Nonnull E entity, @Nonnull Class<? extends IEntity> type) throws DataStoreException {
        return null;
    }

    @Override
    public <E extends IEntity> E update(@Nonnull E entity, @Nonnull Class<? extends IEntity> type) throws DataStoreException {
        return null;
    }

    @Override
    public <E extends IEntity> boolean delete(@Nonnull Object key, @Nonnull Class<? extends E> type) throws DataStoreException {
        return false;
    }

    @Override
    public <E extends IEntity> E find(@Nonnull Object key, @Nonnull Class<? extends E> type) throws DataStoreException {
        return null;
    }

    @Override
    public <E extends IEntity> Collection<E> search(@Nonnull String query, int offset, int maxResults, @Nonnull Class<? extends E> type) throws DataStoreException {
        return null;
    }

    @Override
    public <E extends IEntity> Collection<E> search(@Nonnull String query, int offset, int maxResults, Map<String, Object> params, @Nonnull Class<? extends E> type) throws DataStoreException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public <T> List<T> textSearch(@Nonnull Query query, @Nonnull Class<? extends T> type) throws DataStoreException {
        return null;
    }

    @Override
    public <T> List<T> textSearch(@Nonnull Query query, int batchSize, int offset, @Nonnull Class<? extends T> type) throws DataStoreException {
        return null;
    }

    @Override
    public <T> List<T> textSearch(@Nonnull String query, @Nonnull Class<? extends T> type) throws DataStoreException {
        return null;
    }

    @Override
    public <T> List<T> textSearch(@Nonnull String query, int batchSize, int offset, @Nonnull Class<? extends T> type) throws DataStoreException {
        return null;
    }
}
