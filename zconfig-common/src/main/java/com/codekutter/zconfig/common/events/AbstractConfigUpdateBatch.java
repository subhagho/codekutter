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
 * Date: 4/3/19 4:50 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.events;

import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Abstract base class to define updates.
 *
 * @param <T>
 */
@Getter
@Setter
public class AbstractConfigUpdateBatch<D, T extends AbstractConfigUpdateEvent<D>> {
    /**
     * Config Update Header for this batch.
     */
    protected ConfigUpdateHeader header;
    /**
     * List of update events in the batch.
     */
    protected List<T> events;

    /**
     * Get the size of this event batch.
     *
     * @return - Batch Size.
     */
    public int size() {
        if (events != null) {
            return events.size();
        }
        return 0;
    }

    /**
     * Validate this update batch for consistency.
     *
     * @throws ConfigurationException - Will throw exception if inconsistency.
     */
    public void validate() throws ConfigurationException {
        Preconditions.checkNotNull(header);
        Preconditions.checkNotNull(events);

        String configName = header.getConfigName();
        Preconditions.checkState(!Strings.isNullOrEmpty(configName));
        String group = header.getGroup();
        Preconditions.checkState(!Strings.isNullOrEmpty(group));
        String application = header.getApplication();
        Preconditions.checkState(!Strings.isNullOrEmpty(application));
        String prevVersion = header.getPreVersion();
        Preconditions.checkState(!Strings.isNullOrEmpty(prevVersion));
        try {
            for (AbstractConfigUpdateEvent event : events) {
                Preconditions.checkState(event.getHeader() != null);
                if (!header.equals(event.getHeader())) {
                    throw new ConfigurationException(String.format("Event Header mis-match: [expected=%s][actual=%s]",
                            header.toString(), event.getHeader().toString()));
                }
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }
}
