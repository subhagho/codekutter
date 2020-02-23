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

import com.codekutter.common.Context;
import com.codekutter.common.model.IEntity;
import org.apache.lucene.search.Query;

import javax.annotation.Nonnull;

@SuppressWarnings("rawtypes")
public interface ISearchable {
    <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull Query query,
                                                       @Nonnull Class<? extends T> type,
                                                       Context context) throws DataStoreException;

    <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull Query query,
                                                       int batchSize,
                                                       int offset,
                                                       @Nonnull Class<? extends T> type,
                                                       Context context) throws DataStoreException;

    <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull String query,
                                                       @Nonnull Class<? extends T> type,
                                                       Context context) throws DataStoreException;

    <T extends IEntity> BaseSearchResult<T> textSearch(@Nonnull String query,
                                                       int batchSize,
                                                       int offset,
                                                       @Nonnull Class<? extends T> type,
                                                       Context context) throws DataStoreException;
    <T extends IEntity> BaseSearchResult<T> facetedSearch(@Nonnull Object query,
                                                          @Nonnull Class<? extends T> type,
                                                          Context context) throws DataStoreException;
}
