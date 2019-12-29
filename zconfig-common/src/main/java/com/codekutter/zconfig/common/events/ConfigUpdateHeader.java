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
 * Date: 4/3/19 1:55 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.events;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * Class to define the header of a configuration update transaction batch.
 */
@Getter
@Setter
public class ConfigUpdateHeader {
    /**
     * Application Group name.
     */
    private String group;
    /**
     * Application name.
     */
    private String application;
    /**
     * Configuration name this batch is for.
     */
    private String configName;
    /**
     * Pre-Update version (base version) of the configuration.
     */
    private String preVersion;
    /**
     * Updated version post applying changes.
     */
    private String updatedVersion;
    /**
     * Transaction ID of this update batch.
     */
    private String transactionId;
    /**
     * Timestamp of the update.
     */
    private long timestamp;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConfigUpdateHeader)) return false;
        ConfigUpdateHeader that = (ConfigUpdateHeader) o;
        return timestamp == that.timestamp &&
                group.equals(that.group) &&
                application.equals(that.application) &&
                configName.equals(that.configName) &&
                preVersion.equals(that.preVersion) &&
                updatedVersion.equals(that.updatedVersion) &&
                transactionId.equals(that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, application, configName, preVersion, updatedVersion, transactionId, timestamp);
    }

    @Override
    public String toString() {
        return "ConfigUpdateHeader{" +
                "group='" + group + '\'' +
                ", application='" + application + '\'' +
                ", configName='" + configName + '\'' +
                ", preVersion='" + preVersion + '\'' +
                ", updatedVersion='" + updatedVersion + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
