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

package com.codekutter.common.auditing;

import com.codekutter.common.model.AuditRecord;
import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.IKey;
import com.codekutter.common.model.IKeyed;
import com.codekutter.common.stores.AbstractDataStore;
import com.codekutter.common.stores.BaseSearchResult;
import com.codekutter.common.stores.impl.EntitySearchResult;
import com.codekutter.common.utils.LogUtils;
import com.google.common.base.Preconditions;
import org.hibernate.Session;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;

public class DBAuditLogger extends AbstractAuditLogger<Session> {
    /**
     * Search for entity records based on the query string specified.
     *
     * @param query      - Query String
     * @param entityType - Record Entity type.
     * @param serializer - Entity data serializer
     * @return - Collection of fetched records.
     * @throws AuditException
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T extends IKeyed> Collection<T> search(@Nonnull String query,
                                                   @Nonnull Class<? extends T> entityType,
                                                   @Nonnull IAuditSerDe serializer) throws AuditException {
        Preconditions.checkState(dataStoreManager() != null);
        try {
            state().check(EObjectState.Available, getClass());
            AbstractDataStore<Session> dataStore = getDataStore(false);

            String qstr = String.format("FROM %s WHERE id.recordType = :recordType AND (%s)",
                    AuditRecord.class.getCanonicalName(), query);
            Map<String, Object> params = new HashMap<>();
            params.put("recordType", entityType.getCanonicalName());
            BaseSearchResult<AuditRecord> result = dataStore.search(qstr, params, AuditRecord.class, null);
            if (result instanceof EntitySearchResult) {
                EntitySearchResult<AuditRecord> er = (EntitySearchResult<AuditRecord>) result;
                Collection<AuditRecord> records = er.getEntities();
                if (records != null && !records.isEmpty()) {
                    List<T> entities = new ArrayList<>(records.size());
                    for (AuditRecord record : records) {
                        T entity = (T) serializer.deserialize(record.getEntityData(), entityType);
                        LogUtils.debug(getClass(), entity);
                        entities.add(entity);
                    }
                    return entities;
                }
            }
            return null;
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    /**
     * Fetch all audit records for the specified entity type and entity key.
     *
     * @param key        - Entity Key
     * @param entityType - Entity Type
     * @return - List of audit records.
     * @throws AuditException
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K extends IKey, T extends IKeyed<K>> Collection<AuditRecord> find(@Nonnull K key,
                                                                              @Nonnull Class<? extends T> entityType) throws AuditException {
        Preconditions.checkState(dataStoreManager() != null);
        try {
            state().check(EObjectState.Available, getClass());
            AbstractDataStore<Session> dataStore = getDataStore(false);

            String qstr = String.format("FROM %s WHERE id.recordType = :recordType AND entityId = :entityId",
                    AuditRecord.class.getCanonicalName());
            Map<String, Object> params = new HashMap<>();
            params.put("recordType", entityType.getCanonicalName());
            params.put("entityId", key.toString());
            BaseSearchResult<AuditRecord> result = dataStore.search(qstr, params, AuditRecord.class, null);
            if (result instanceof EntitySearchResult) {
                EntitySearchResult<AuditRecord> er = (EntitySearchResult<AuditRecord>) result;
                Collection<AuditRecord> records = er.getEntities();
                if (records != null && !records.isEmpty()) {
                    return records;
                }
            }
            return null;
        } catch (Throwable ex) {
            throw new AuditException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (state().getState() == EObjectState.Available) {
            state().setState(EObjectState.Disposed);
        }
    }
}
