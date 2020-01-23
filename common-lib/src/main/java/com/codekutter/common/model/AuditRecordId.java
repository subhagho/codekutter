package com.codekutter.common.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Getter
@Setter
@ToString
@Embeddable
public class AuditRecordId implements IKey, Serializable {
    /**
     * Audit Record Type - Record Entity Class
     */
    @Column(name = "record_type")
    private String recordType;
    /**
     * Audit Record Id - Unique Record ID
     */
    @Column(name = "record_id")
    private String recordId;

    public int compareTo(AuditRecordId id) {
        int ret = recordType.compareTo(id.recordType);
        if (ret == 0) {
            ret = recordId.compareTo(id.recordId);
        }
        return ret;
    }

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        return String.format("%s::%s", recordType, recordId);
    }

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    @Override
    public int compareTo(IKey key) {
        if (key instanceof AuditRecordId) {
            AuditRecordId k = (AuditRecordId)key;
            int ret = recordType.compareTo(k.recordType);
            if (ret == 0) {
                ret = recordId.compareTo(k.recordId);
            }
            return ret;
        }
        return -1;
    }
}
