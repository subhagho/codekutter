package com.codekutter.common.auditing;

import com.codekutter.common.StateException;
import com.codekutter.common.model.*;
import com.codekutter.common.stores.AbstractDataStore;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.DataStoreManager;
import com.codekutter.common.stores.TransactionDataStore;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.MapThreadCache;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.transformers.StringListParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.Session;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class to define Audit loggers.
 *
 * @param <C> - Data Store Connection type.
 */
@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "audit-logger")
public abstract class AbstractAuditLogger<C> implements IConfigurable, Closeable {
    private static final int MAX_CACHE_SIZE = 32;

    @ConfigAttribute(required = true)
    private String name;
    @ConfigValue(name = "serializer")
    private Class<? extends IAuditSerDe> serializerClass;
    @ConfigAttribute(name = "default")
    private boolean defaultLogger = false;
    @ConfigAttribute(name = "dataStore@name", required = true)
    private String dataStoreName;
    @ConfigAttribute(name = "dataStore@class", required = true)
    private Class<? extends AbstractDataStore> dataStoreType;
    @ConfigValue(name = "classes", parser = StringListParser.class)
    private List<String> classes;
    @ConfigAttribute
    private boolean useCache = false;
    @ConfigAttribute
    private int maxCacheSize = MAX_CACHE_SIZE;
    @Setter(AccessLevel.NONE)
    private DataStoreManager dataStoreManager;
    @Setter(AccessLevel.NONE)
    private IAuditSerDe serializer;
    @Setter(AccessLevel.NONE)
    private ObjectState state = new ObjectState();
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private MapThreadCache<String, AuditRecord> cache;

    /**
     * Set the data store to be used by this audit logger.
     *
     * @param dataStoreManager - Data Store handle.
     * @return - Self
     */
    public AbstractAuditLogger<C> withDataStoreManager(@Nonnull DataStoreManager dataStoreManager) {
        this.dataStoreManager = dataStoreManager;
        return this;
    }

    /**
     * Set the data serializer for this logger.
     *
     * @param serializer - Entity data serializer.
     * @return - Self
     */
    public AbstractAuditLogger<C> withSerializer(@Nonnull IAuditSerDe serializer) {
        this.serializer = serializer;
        return this;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public AbstractDataStore<C> getDataStore(boolean checkTransaction) throws AuditException, DataStoreException {
        AbstractDataStore<C> dataStore =
                dataStoreManager().getDataStore(dataStoreName(), (Class<? extends AbstractDataStore<C>>) dataStoreType());
        if (dataStore == null) {
            throw new AuditException(String.format("Data Store not found. [type=%s][name=%s]", dataStoreType().getCanonicalName(), dataStoreName()));
        }
        if (checkTransaction && dataStore.connection().hasTransactionSupport()) {
            TransactionDataStore ts = (TransactionDataStore) dataStore;
            if (!ts.isInTransaction()) {
                ts.beingTransaction();
            }
        }
        return dataStore;
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
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            if (serializerClass() != null) {
                IAuditSerDe serializer = serializerClass().newInstance();
                withSerializer(serializer);
                LogUtils.info(getClass(), String.format("Using default serializer. [type=%s]", serializer.getClass().getCanonicalName()));
            }
            if (useCache) {
                cache = new MapThreadCache<>();
                LogUtils.debug(getClass(), String.format("Using audit cache. [cache size=%d]", maxCacheSize));
            }
            state().setState(EObjectState.Available);
            LogUtils.info(getClass(), String.format("Initialized DataBase Audit Logger. [name=%s]", name()));
        } catch (Throwable ex) {
            state().setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    /**
     * Write an audit record for the audit type and specified entity record.
     * Will use the default serializer if set, else throw exception.
     *
     * @param type       - Audit record type.
     * @param entity     - Entity to audit
     * @param entityType - Class of the the entity.
     * @param user       - User principal
     * @param <T>        - Entity record type.
     * @return - Created Audit record.
     * @throws AuditException
     */
    public <T extends IKeyed> AuditRecord write(@Nonnull Class<?> dataStoreType,
                                                @Nonnull String dataStoreName,
                                                @Nonnull EAuditType type,
                                                @Nonnull T entity,
                                                @Nonnull Class<? extends T> entityType,
                                                String changeDelta,
                                                String changeContext,
                                                @Nonnull Principal user) throws AuditException {
        try {
            state.check(EObjectState.Available, getClass());
            if (serializer == null) {
                throw new AuditException(String.format("[logger=%s] No serializer defined.", getClass().getCanonicalName()));
            }
            return write(dataStoreType, dataStoreName, type, entity, entityType, changeDelta, changeContext, user, serializer);
        } catch (StateException ex) {
            throw new AuditException(ex);
        }
    }

    /**
     * Write an audit record for the audit type and specified entity record.
     * Will use the default serializer if set, else throw exception.
     *
     * @param type       - Audit record type.
     * @param entity     - Entity to audit
     * @param entityType - Class of the the entity.
     * @param user       - User principal
     * @param serializer - Record serializer to use.
     * @param <T>        - Entity record type.
     * @return - Created Audit record.
     * @throws AuditException
     */
    public <T extends IKeyed> AuditRecord write(@Nonnull Class<?> dataStoreType,
                                                @Nonnull String dataStoreName,
                                                @Nonnull EAuditType type,
                                                @Nonnull T entity,
                                                @Nonnull Class<? extends T> entityType,
                                                String changeDelta,
                                                String changeContext,
                                                @Nonnull Principal user,
                                                @Nonnull IAuditSerDe serializer) throws AuditException {
        Preconditions.checkState(dataStoreManager != null);
        try {
            state.check(EObjectState.Available, getClass());
            AuditRecord record = createAuditRecord(dataStoreType, dataStoreName, type, entity, entityType, changeDelta, changeContext, user, serializer);
            if (useCache) {
                cache.put(record.getKey().stringKey(), record);
                if (cache.size() > maxCacheSize) {
                    flush();
                }
            } else {
                record = writeToStore(record);
            }
            return record;
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    public <T extends IKeyed> AuditRecord writeToStore(AuditRecord record) throws AuditException {
        Preconditions.checkState(dataStoreManager != null);
        try {
            state.check(EObjectState.Available, getClass());
            AbstractDataStore<C> dataStore = getDataStore(true);
            record = dataStore.createEntity(record, record.getClass(), null);
            return record;
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    public void discard() throws AuditException {
        try {
            if (useCache) {
                cache.clear();
                LogUtils.debug(getClass(), "Discarded cached audit records...");
            }
            AbstractDataStore<C> dataStore = getDataStore(false);
            if (dataStore == null) {
                throw new AuditException(String.format("Data Store not found. [type=%s][name=%s]", dataStoreType().getCanonicalName(), dataStoreName()));
            }
            if (dataStore.connection().hasTransactionSupport()) {
                TransactionDataStore ts = (TransactionDataStore) dataStore;
                if (ts.isInTransaction()) {
                    ts.rollback();
                }
            }
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    public int flush() throws AuditException {
        try {
            int size = 0;
            if (useCache) {
                Map<String, AuditRecord> records = cache.get();
                if (records != null) {
                    size = records.size();
                    if (size > 0) {
                        for (String key : records.keySet()) {
                            AuditRecord record = records.get(key);
                            writeToStore(record);
                        }
                    }
                    cache.clear();
                    LogUtils.debug(getClass(), String.format("Flushed [%d] audit records to store.", size));
                }
            }
            AbstractDataStore<C> dataStore = getDataStore(false);
            if (dataStore == null) {
                throw new AuditException(String.format("Data Store not found. [type=%s][name=%s]", dataStoreType().getCanonicalName(), dataStoreName()));
            }
            if (dataStore.connection().hasTransactionSupport()) {
                TransactionDataStore ts = (TransactionDataStore) dataStore;
                if (!ts.isInTransaction()) {
                    ts.commit();
                }
            }
            return size;
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    @SuppressWarnings("unchecked")
    protected <T extends IKeyed> AuditRecord createAuditRecord(@Nonnull Class<?> dataStoreType,
                                                               @Nonnull String dataStoreName,
                                                               @Nonnull EAuditType type,
                                                               @Nonnull T entity,
                                                               @Nonnull Class<? extends T> entityType,
                                                               String changeDelta,
                                                               String changeContext,
                                                               @Nonnull Principal user,
                                                               @Nonnull IAuditSerDe serializer) throws AuditException {
        try {
            AuditRecord record = new AuditRecord(dataStoreType, dataStoreName, entityType, user.getName());
            record.setAuditType(type);
            byte[] data = serializer.serialize(entity, entityType);
            record.setEntityData(data);
            record.setEntityId(entity.getKey().stringKey());
            if (!Strings.isNullOrEmpty(changeDelta)) {
                record.setChangeDelta(changeDelta.getBytes(StandardCharsets.UTF_8));
            }
            if (!Strings.isNullOrEmpty(changeContext)) {
                record.setChangeContext(changeContext.getBytes(StandardCharsets.UTF_8));
            }
            return record;
        } catch (Throwable ex) {
            throw new AuditException(ex);
        }
    }

    /**
     * Extract and fetch the entities from the audit records
     * retrieved by the entity key and entity type.
     *
     * @param key        - Entity Key
     * @param entityType - Entity Type
     * @param <K>        - Entity Key Type
     * @param <T>        - Entity Type.
     * @return - List of extracted entity records.
     * @throws AuditException
     */
    public <K extends IKey, T extends IKeyed<K>> List<T> fetch(@Nonnull K key,
                                                               @Nonnull Class<? extends T> entityType) throws AuditException {
        Preconditions.checkState(serializer != null);
        return fetch(key, entityType, serializer);
    }

    /**
     * Extract and fetch the entities from the audit records
     * retrieved by the entity key and entity type.
     *
     * @param key        - Entity Key
     * @param entityType - Entity Type
     * @param serializer - Entity data serializer
     * @param <K>        - Entity Key Type
     * @param <T>        - Entity Type.
     * @return - List of extracted entity records.
     * @throws AuditException
     */
    @SuppressWarnings("unchecked")
    public <K extends IKey, T extends IKeyed<K>> List<T> fetch(@Nonnull K key,
                                                               @Nonnull Class<? extends T> entityType,
                                                               @Nonnull IAuditSerDe serializer) throws AuditException {
        Preconditions.checkState(dataStoreManager != null);
        try {
            state.check(EObjectState.Available, getClass());
            Collection<AuditRecord> records = find(key, entityType);
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
        } catch (StateException ex) {
            throw new AuditException(ex);
        }
    }

    /**
     * Search for entity records based on the query string specified.
     *
     * @param query      - Query String
     * @param entityType - Record Entity type.
     * @param <T>        - Entity Type
     * @return - Collection of fetched records.
     * @throws AuditException
     */
    public <T extends IKeyed> Collection<T> search(@Nonnull String query,
                                                   @Nonnull Class<? extends T> entityType) throws AuditException {
        Preconditions.checkState(serializer != null);
        return search(query, entityType, serializer);
    }

    /**
     * Search for entity records based on the query string specified.
     *
     * @param query      - Query String
     * @param entityType - Record Entity type.
     * @param serializer - Entity data serializer
     * @param <T>        - Entity Type
     * @return - Collection of fetched records.
     * @throws AuditException
     */
    public abstract <T extends IKeyed> Collection<T> search(@Nonnull String query,
                                                            @Nonnull Class<? extends T> entityType,
                                                            @Nonnull IAuditSerDe serializer) throws AuditException;

    /**
     * Fetch all audit records for the specified entity type and entity key.
     *
     * @param key        - Entity Key
     * @param entityType - Entity Type
     * @param <K>        - Entity Key Type
     * @param <T>        - Entity Type.
     * @return - List of audit records.
     * @throws AuditException
     */
    public abstract <K extends IKey, T extends IKeyed<K>> Collection<AuditRecord> find(@Nonnull K key,
                                                                                       @Nonnull Class<? extends T> entityType) throws AuditException;
}
