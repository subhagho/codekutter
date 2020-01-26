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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import javax.persistence.Embeddable;
import java.io.Serializable;

/**
 * Class represents a asset modification update.
 * Includes the modified by and modification timestamp.
 */
@Embeddable
public class ModifiedBy extends ModificationLog<String> implements Serializable {
    public ModifiedBy() {}

    public ModifiedBy(@Nonnull String userId) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(userId));
        setModifiedBy(userId);
        setTimestamp(System.currentTimeMillis());
    }

    public ModifiedBy(@Nonnull ModifiedBy source) {
        setModifiedBy(source.getModifiedBy());
        setTimestamp(source.getTimestamp());
    }
}
