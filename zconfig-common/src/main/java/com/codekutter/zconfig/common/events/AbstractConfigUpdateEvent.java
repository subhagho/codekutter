/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright (c) $year
 * Date: 4/3/19 8:40 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.events;

import lombok.Getter;
import lombok.Setter;

/**
 * Update event structure for a configuration node update.
 * <p>
 * Event Structure:
 * <pre>
 *      {
 *           "transaction" : [unique tnx ID],
 *           "group" : [application group],
 *           "application" : [application],
 *           "config" : [config name],
 *           "version" : [config version],
 *           "path" : [node path],
 *           "eventType" : [ADD/UPDATE/DELETE],
 *           "timestamp" : [timestamp]
 *       }
 *  </pre>
 */
@Getter
@Setter
public abstract class AbstractConfigUpdateEvent<T> {
    /**
     * Update header for this event.
     */
    private ConfigUpdateHeader header;
    /**
     * Type of update event.
     */
    private EUpdateEventType eventType;

    /**
     * Node path of the node being updated.
     */
    private String path;

    /**
     * Sequence of this event in the transaction block.
     */
    private long transactionSequence;
    /**
     * Event timestamp of the transaction.
     */
    private long timestamp;

    /**
     * Event Data.
     */
    private T value;
}
