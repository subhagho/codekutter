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

package com.codekutter.zconfig.common.model;

import com.codekutter.zconfig.common.model.nodes.EValueType;
import lombok.Data;

import javax.persistence.Version;
import javax.persistence.*;

@Data
@Entity
@Table(name = "tb_config_records")
public class ConfigDbRecord {
    @EmbeddedId
    private ConfigDbRecordId id;
    @Column(name = "minor_version")
    private int minorVersion;
    @Enumerated(EnumType.STRING)
    @Column(name = "value_type")
    private EValueType valueType;
    @Column(name = "encrypted")
    private boolean encrypted = false;
    @Column(name = "value")
    private String value;
    @Column(name = "modified_timestamp")
    private long modifiedTimestamp;
    @Version
    @Column(name = "record_version")
    private long recordVersion;
}
