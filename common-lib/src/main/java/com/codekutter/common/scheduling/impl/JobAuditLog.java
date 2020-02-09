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

package com.codekutter.common.scheduling.impl;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Getter
@Setter
@Entity
@Table(name = "tb_job_audit")
public class JobAuditLog {
    @Id
    @Column(name = "job_id")
    private String jobId;
    @Column(name = "namespace")
    private String namespace;
    @Column(name = "name")
    private String name;
    @Column(name = "job_type")
    private String type;
    @Column(name = "start_timestamp")
    private long startTime;
    @Column(name = "end_timestamp")
    private long endTime;
    @Column(name = "context_json")
    private String contextJson;
    @Column(name = "response_json")
    private String responseJson;
    @Column(name = "error")
    private String error;
    @Column(name = "error_trace")
    private String errorTrace;
}
