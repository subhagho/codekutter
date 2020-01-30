package com.codekutter.common.auditing;

import com.codekutter.common.model.AuditRecord;
import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.IKey;
import com.codekutter.common.model.IKeyed;
import com.codekutter.common.stores.AbstractDataStore;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.TransactionDataStore;
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
            Collection<AuditRecord> records = dataStore.search(qstr, params, AuditRecord.class, null);
            if (records != null && !records.isEmpty()) {
                List<T> entities = new ArrayList<>(records.size());
                for (AuditRecord record : records) {
                    T entity = (T) serializer.deserialize(record.getEntityData(), entityType);
                    LogUtils.debug(getClass(), entity);
                    entities.add(entity);
                }
                return entities;
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
            Collection<AuditRecord> records = dataStore.search(qstr, params, AuditRecord.class, null);
            if (records != null && !records.isEmpty()) {
                return records;
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
