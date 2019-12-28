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
 * Date: 1/1/19 6:43 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.model;

import com.codekutter.common.utils.DateTimeUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

/**
 * Class represents a asset modification update.
 * Includes the modified by and modification timestamp.
 */
public class ModifiedBy {
    /**
     * User ID this asset was modified by.
     */
    private String modifiedBy;
    /**
     * Timestamp of the modification.
     */
    private long timestamp;

    public ModifiedBy() {}

    public ModifiedBy(@Nonnull String userId) {
        modifiedBy = userId;
        timestamp = System.currentTimeMillis();
    }

    public ModifiedBy(@Nonnull ModifiedBy source) {
        this.modifiedBy = source.modifiedBy;
        this.timestamp = source.timestamp;
    }
    /**
     * Get the modified by User ID.
     *
     * @return - Modified By user ID.
     */
    public String getModifiedBy() {
        return modifiedBy;
    }

    /**
     * Set the modified by User ID.
     *
     * @param modifiedBy - Modified By user ID.
     */
    public void setModifiedBy(String modifiedBy) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(modifiedBy));
        this.modifiedBy = modifiedBy;
    }

    /**
     * Get the modified at timestamp.
     *
     * @return - Modified at timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Set the modified at timestamp.
     *
     * @param timestamp - Modified at timestamp.
     */
    public void setTimestamp(long timestamp) {
        Preconditions.checkNotNull(timestamp);
        this.timestamp = timestamp;
    }

    /**
     * Override the default toString method to print the user/timestamp.
     *
     * @return - To String
     */
    @Override
    public String toString() {
        return String.format("{user=%s, timestamp=%s}", modifiedBy,
                             DateTimeUtils.toString(timestamp));
    }
}
