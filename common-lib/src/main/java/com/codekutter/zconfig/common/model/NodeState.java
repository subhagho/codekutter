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
 * Date: 1/1/19 9:34 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.model;

import com.codekutter.common.AbstractState;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * State object used to represent the state of a configuration node.
 *
 */
public class NodeState extends AbstractState<ENodeState> {
    /**
     * Is this a newly created node?
     *
     * @return - Is new?
     */
    @JsonIgnore
    public boolean isNew() {
        return (getState() == ENodeState.New);
    }

    /**
     * Is this node in a synced state? (synced with backend)
     *
     * @return - Is Synced?
     */
    @JsonIgnore
    public boolean isSynced() {
        return (getState() == ENodeState.Synced);
    }

    /**
     * Has this node been updated?
     *
     * @return - Is updated?
     */
    @JsonIgnore
    public boolean isUpdated() {
        return (getState() == ENodeState.Updated);
    }

    /**
     * Is this node in a loading state?
     *
     * @return - Is loading?
     */
    @JsonIgnore
    public boolean isLoading() {
        return (getState() == ENodeState.Loading);
    }

    /**
     * Has this node been deleted?
     *
     * @return - Is deleted?
     */
    @JsonIgnore
    public boolean isDeleted() {
        return (getState() == ENodeState.Deleted);
    }
}
