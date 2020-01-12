package com.codekutter.common.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Embeddable;
import java.io.Serializable;

@Getter
@Setter
@ToString
@Embeddable
public class AuditRecordId implements Serializable {
    /**
     * Audit Record Type - Record Entity Class
     */
    private String recordType;
    /**
     * Audit Record Id - Unique Record ID
     */
    private String recordId;

    public int compareTo(AuditRecordId id) {
        int ret = recordType.compareTo(id.recordType);
        if (ret == 0) {
            ret = recordId.compareTo(id.recordId);
        }
        return ret;
    }
}
