package com.codekutter.common.model;

import com.codekutter.common.Context;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.*;
import java.util.UUID;

@Getter
@Setter
@Entity
@Table(name = "tb_audit_records")
public class AuditRecord implements IEntity<AuditRecordId> {
    @EmbeddedId
    private AuditRecordId id;
    @Enumerated(EnumType.STRING)
    @Column(name = "audit_type")
    private EAuditType auditType;
    @Column(name = "entity_id")
    private String entityId;
    @Column(name = "entity_data")
    private byte[] entityData;
    @AttributeOverrides({
            @AttributeOverride(name = "modifiedBy", column = @Column(name = "user_id")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "timestamp"))
    })
    private ModifiedBy createdBy;

    public AuditRecord() {}

    public AuditRecord(@Nonnull Class<?> entityType) {
        id = new AuditRecordId();
        id.setRecordType(entityType.getCanonicalName());
        id.setRecordId(UUID.randomUUID().toString());
    }

    public AuditRecord(@Nonnull Class<?> entityType, @Nonnull String userId) {
        id = new AuditRecordId();
        id.setRecordType(entityType.getCanonicalName());
        id.setRecordId(UUID.randomUUID().toString());

        createdBy = new ModifiedBy();
        createdBy.setModifiedBy(userId);
        createdBy.setTimestamp(System.currentTimeMillis());
    }

    /**
     * Get the unique Key for this entity.
     *
     * @return - Entity Key.
     */
    @Override
    public AuditRecordId getKey() {
        return id;
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(AuditRecordId key) {
        return id.compareTo(key);
    }

    /**
     * Copy the changes from the specified source entity
     * to this instance.
     * <p>
     * All properties other than the Key will be copied.
     * Copy Type:
     * Primitive - Copy
     * String - Copy
     * Enum - Copy
     * Nested Entity - Copy Recursive
     * Other Objects - Copy Reference.
     *
     * @param source  - Source instance to Copy from.
     * @param context - Execution context.
     * @return - Copied Entity instance.
     * @throws CopyException
     */
    @Override
    public IEntity<AuditRecordId> copyChanges(IEntity<AuditRecordId> source, Context context) throws CopyException {
        return this;
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<AuditRecordId> clone(Context context) throws CopyException {
        return this;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        ValidationExceptions errors = null;
        if (id == null) {
            errors = ValidationExceptions.add(new ValidationException("Record ID is null."), errors);
        } else if (Strings.isNullOrEmpty(id.getRecordType())) {
            errors = ValidationExceptions.add(new ValidationException("Invalid Record ID: Entity record type is NULL/Empty."), errors);
        }
        if (createdBy == null) {
            errors = ValidationExceptions.add(new ValidationException("Created By is null."), errors);
        } else if (Strings.isNullOrEmpty(createdBy.getModifiedBy())) {
            errors = ValidationExceptions.add(new ValidationException("Invalid Created By: User ID is null/empty."), errors);
        }
        if (auditType == null) {
            errors = ValidationExceptions.add(new ValidationException("Audit Record Type is null"), errors);
        }
        if (errors != null) {
            throw errors;
        }
    }
}
