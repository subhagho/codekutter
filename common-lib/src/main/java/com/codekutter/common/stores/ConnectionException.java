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
 * Date: 9/2/19 11:12 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.stores;

/**
 * Exception instance used to raise error for state checks.
 */
public class ConnectionException extends Exception {
    private static final String PREFIX = "[%s] Connection Error : %s";

    /**
     * Exception constructor with error message string.
     *
     * @param s - Error message string.
     */
    public ConnectionException(String s, Class<? extends AbstractConnection<?>> type) {
        super(String.format(PREFIX, type.getCanonicalName(), s));
    }

    /**
     * Exception constructor with error message string and inner cause.
     *
     * @param s         - Error message string.
     * @param throwable - Inner cause.
     */
    public ConnectionException(String s, Throwable throwable, Class<? extends AbstractConnection<?>> type) {
        super(String.format(PREFIX, type.getCanonicalName(), s), throwable);
    }

    /**
     * Exception constructor inner cause.
     *
     * @param throwable - Inner cause.
     */
    public ConnectionException(Throwable throwable, Class<? extends AbstractConnection<?>> type) {
        super(String.format(PREFIX, type.getCanonicalName(), throwable.getLocalizedMessage()), throwable);
    }
}
