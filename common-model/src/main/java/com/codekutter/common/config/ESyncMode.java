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
 * Date: 17/2/19 9:22 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.config;

/**
 * Enum for specifying the sync mode for configuration updates.
 */
public enum ESyncMode {
    /**
     * Manual Sync - Client needs to sync the entire configuration manually.
     */
    MANUAL,
    /**
     * Batch Sync - Updates published in incremental batches.
     */
    BATCH,
    /**
     * Events Sync - Update events are published per committed transaction.
     */
    EVENTS
}
