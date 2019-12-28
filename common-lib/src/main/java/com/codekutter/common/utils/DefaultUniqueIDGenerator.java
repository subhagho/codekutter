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
 * Date: 17/2/19 11:11 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.utils;


import com.codekutter.common.Context;

import java.util.UUID;

public class DefaultUniqueIDGenerator implements IUniqueIDGenerator {
    /**
     * Generate a Unique String ID.
     *
     * @param context - Additional parameter context, if required.
     * @return - Generated String ID.
     */
    @Override
    public String generateStringId(Context context) {
        return UUID.randomUUID().toString();
    }

    /**
     * Generate a Unique Integer ID.
     *
     * @param context - Additional parameter context, if required.
     * @return - Generated Integer ID.
     */
    @Override
    public int generateIntId(Context context) {
        throw new RuntimeException("Method generateIntId() not implemented.");
    }

    /**
     * Generate a Unique Long ID.
     *
     * @param context - Additional parameter context, if required.
     * @return - Generated Long ID.
     */
    @Override
    public long generateLongId(Context context) {
        throw new RuntimeException("Method generateIntId() not implemented.");
    }
}
