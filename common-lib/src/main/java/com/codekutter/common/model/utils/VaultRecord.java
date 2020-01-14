package com.codekutter.common.model.utils;

import com.codekutter.common.Context;
import com.codekutter.common.model.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.persistence.*;
import javax.persistence.Version;
import java.util.UUID;

@Data
@Entity
@Table(name = "tb_key_vault")
public class VaultRecord implements IEntity<StringKey> {
    @EmbeddedId
    private StringKey key;
    @Column(name = "data")
    private byte[] data;
    @AttributeOverrides({
            @AttributeOverride(name = "modifiedBy", column = @Column(name = "created_by")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "created_at"))
    })
    private ModifiedBy createdBy;
    @AttributeOverrides({
            @AttributeOverride(name = "modifiedBy", column = @Column(name = "modified_by")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "modified_at"))
    })
    private ModifiedBy modifiedBy;
    @Column(name = "record_version")
    @Version
    private long recordVersion = 0;

    /**
     * Get the unique Key for this entity.
     *
     * @return - Entity Key.
     */
    @Override
    public StringKey getKey() {
        return key;
    }


    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(StringKey key) {
        return this.key.compareTo(key);
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
    public IEntity<StringKey> copyChanges(IEntity<StringKey> source, Context context) throws CopyException {
        Preconditions.checkArgument(source instanceof VaultRecord);
        VaultRecord r = (VaultRecord)source;
        this.data = r.data;
        this.createdBy = r.createdBy;
        this.modifiedBy = r.modifiedBy;

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
    public IEntity<StringKey> clone(Context context) throws CopyException {
        VaultRecord r = new VaultRecord();
        r.key = new StringKey(UUID.randomUUID().toString());
        r.data = this.data;
        r.createdBy = new ModifiedBy(this.createdBy);
        r.modifiedBy = new ModifiedBy(this.modifiedBy);

        return r;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        ValidationExceptions errs = null;
        if (data == null || data.length <= 0)
            errs = ValidationExceptions.add(new ValidationException("Record has no data."), errs);
        if (createdBy == null || Strings.isNullOrEmpty(createdBy.getModifiedBy()))
            errs = ValidationExceptions.add(new ValidationException("Created By not set or Empty."), errs);
        if (modifiedBy == null || Strings.isNullOrEmpty(modifiedBy.getModifiedBy()))
            errs = ValidationExceptions.add(new ValidationException("Modified By not set or Empty."), errs);
        if (errs != null)
            throw errs;
    }
}
