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

import javax.persistence.Version;
import javax.persistence.*;

@Entity
@Table(name = "sys_db_locks")
@Getter
@Setter
public class DbLockRecord {
    @EmbeddedId
    private LockId id;
    @Column(name = "locked")
    private boolean locked = false;
    @Column(name = "instance_id")
    private String instanceId;
    @Column(name = "timestamp")
    private long timestamp;
    @Column(name = "read_lock_count")
    private long readLockCount;
    @Version
    @Column(name = "record_version")
    private long recordVersion;
}
