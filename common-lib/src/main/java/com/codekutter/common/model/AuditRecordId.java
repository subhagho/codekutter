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

package com.codekutter.common.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Getter
@Setter
@ToString
@Embeddable
public class AuditRecordId implements IKey, Serializable {
    /**
     * Data Store type where the operation
     * is being performed.
     */
    @Column(name = "data_store_type")
    private String dataStoreType;
    /**
     * Data Store name where the operation
     * is being performed.
     */
    @Column(name = "data_store_name")
    private String dataStoreName;
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

    public AuditRecordId() {}

    public AuditRecordId(@Nonnull String dataStoreType,
                         @Nonnull String dataStoreName,
                         @Nonnull String recordType,
                         @Nonnull String recordId) {
        this.dataStoreType = dataStoreType;
        this.dataStoreName = dataStoreName;
        this.recordType = recordType;
        this.recordId = recordId;
    }

    public int compareTo(AuditRecordId id) {
        int ret = dataStoreType.compareTo(id.dataStoreType);
        if (ret == 0) {
            ret = dataStoreName.compareTo(id.dataStoreName);
            if (ret == 0) {
                ret = recordType.compareTo(id.recordType);
                if (ret == 0) {
                    ret = recordId.compareTo(id.recordId);
                }
            }
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
        return String.format("%s::%s::%s::%s", dataStoreType, dataStoreName, recordType, recordId);
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
            return compareTo(k);
        }
        return -1;
    }
}
