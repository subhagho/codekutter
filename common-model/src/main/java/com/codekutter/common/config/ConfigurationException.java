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
 * Date: 1/1/19 9:45 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.config;

/**
 * Exception instance used to raise error in processing configuration requests.
 */
public class ConfigurationException extends Exception {
    private static final String PREFIX = "Configuration Exception : %s";
    private static final String PROP_NOT_FOUND_STR =
            "Required configuration property not found. [property=%s]";

    /**
     * Exception constructor with error message string.
     *
     * @param s - Error message string.
     */
    public ConfigurationException(String s) {
        super(String.format(PREFIX, s));
    }

    /**
     * Exception constructor with error message string and inner cause.
     *
     * @param s         - Error message string.
     * @param throwable - Inner cause.
     */
    public ConfigurationException(String s, Throwable throwable) {
        super(String.format(PREFIX, s), throwable);
    }

    /**
     * Exception constructor inner cause.
     *
     * @param throwable - Inner cause.
     */
    public ConfigurationException(Throwable throwable) {
        super(String.format(PREFIX, throwable.getLocalizedMessage()), throwable);
    }

    /**
     * Create a Configuration exception for property not found.
     *
     * @param property - Property name.
     * @return - Configuration Exception.
     */
    public static ConfigurationException propertyNotFoundException(
            String property) {
        String mesg = String.format(PROP_NOT_FOUND_STR, property);
        return new ConfigurationException(mesg);
    }
}
