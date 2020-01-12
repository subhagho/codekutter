package com.codekutter.common.auditing;

import com.codekutter.common.model.AuditRecord;
import com.codekutter.common.model.EAuditType;
import com.codekutter.common.model.IKeyed;
import com.codekutter.common.stores.AbstractDataStore;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.security.Principal;
import java.util.ArrayList;
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
public abstract class AbstractAuditLogger<C> implements IConfigurable {
    @ConfigAttribute(required = true)
    private String name;
    @ConfigValue(name = "serializer")
    private Class<? extends IAuditSerDe> serializerClass;
    @Setter(AccessLevel.NONE)
    private AbstractDataStore<C> dataStore;
    @Setter(AccessLevel.NONE)
    private IAuditSerDe serializer;

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
        if (serializer == null) {
            throw new AuditException(String.format("[logger=%s] No serializer defined.", getClass().getCanonicalName()));
        }
        return write(type, entity, entityType, user, serializer);
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
        List<AuditRecord> records = find(key, entityType, serializer);
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
    }

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
    public <K, T extends IKeyed<K>> List<AuditRecord> find(@Nonnull K key,
                                                           @Nonnull Class<? extends T> entityType) throws AuditException {
        Preconditions.checkState(serializer != null);
        return find(key, entityType, serializer);
    }

    /**
     * Fetch all audit records for the specified entity type and entity key.
     *
     * @param key        - Entity Key
     * @param entityType - Entity Type
     * @param serializer - Entity data serializer
     * @param <K>        - Entity Key Type
     * @param <T>        - Entity Type.
     * @return - List of audit records.
     * @throws AuditException
     */
    public abstract <K, T extends IKeyed<K>> List<AuditRecord> find(@Nonnull K key,
                                                                    @Nonnull Class<? extends T> entityType,
                                                                    @Nonnull IAuditSerDe serializer) throws AuditException;
}
