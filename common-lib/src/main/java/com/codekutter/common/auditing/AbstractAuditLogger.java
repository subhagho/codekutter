package com.codekutter.common.auditing;

import com.codekutter.common.StateException;
import com.codekutter.common.model.*;
import com.codekutter.common.stores.AbstractDataStore;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
    @Setter(AccessLevel.NONE)
    private AbstractDataStore<C> dataStore;
    @Setter(AccessLevel.NONE)
    private IAuditSerDe serializer;
    @Setter(AccessLevel.NONE)
    private ObjectState state = new ObjectState();

    /**
     * Set the data store to be used by this audit logger.
     *
     * @param dataStore - Data Store handle.
     * @return - Self
     */
    public AbstractAuditLogger<C> withDataStore(@Nonnull AbstractDataStore<C> dataStore) {
        this.dataStore = dataStore;
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
    public <T extends IKeyed> AuditRecord write(@Nonnull EAuditType type,
                                                @Nonnull T entity,
                                                @Nonnull Class<? extends T> entityType,
                                                @Nonnull Principal user) throws AuditException {
        try {
            state.check(EObjectState.Available, getClass());
            if (serializer == null) {
                throw new AuditException(String.format("[logger=%s] No serializer defined.", getClass().getCanonicalName()));
            }
            return write(type, entity, entityType, user, serializer);
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
    public <T extends IKeyed> AuditRecord write(@Nonnull EAuditType type,
                                                @Nonnull T entity,
                                                @Nonnull Class<? extends T> entityType,
                                                @Nonnull Principal user,
                                                @Nonnull IAuditSerDe serializer) throws AuditException {
        Preconditions.checkState(dataStore != null);
        try {
            state.check(EObjectState.Available, getClass());
            AuditRecord record = createAuditRecord(type, entity, entityType, user, serializer);
            record = dataStore.create(record, record.getClass(), null);
            return record;
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    @SuppressWarnings("unchecked")
    protected <T extends IKeyed> AuditRecord createAuditRecord(@Nonnull EAuditType type,
                                                               @Nonnull T entity,
                                                               @Nonnull Class<? extends T> entityType,
                                                               @Nonnull Principal user,
                                                               @Nonnull IAuditSerDe serializer) throws AuditException {
        try {
            AuditRecord record = new AuditRecord(entityType, user.getName());
            record.setAuditType(type);
            byte[] data = serializer.serialize(entity, entityType);
            record.setEntityData(data);
            record.setEntityId(entity.getStringKey());

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
    public <K, T extends IKeyed<K>> List<T> fetch(@Nonnull K key,
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
    public <K, T extends IKeyed<K>> List<T> fetch(@Nonnull K key,
                                                  @Nonnull Class<? extends T> entityType,
                                                  @Nonnull IAuditSerDe serializer) throws AuditException {
        Preconditions.checkState(dataStore != null);
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
    public abstract <K, T extends IKeyed<K>> Collection<AuditRecord> find(@Nonnull K key,
                                                                          @Nonnull Class<? extends T> entityType) throws AuditException;
}
