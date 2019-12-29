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
 * Date: 10/2/19 8:59 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.AbstractState;
import com.codekutter.common.StateException;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * State instance used for managing client states.
 */
public class ClientState extends AbstractState<EClientState> {
    /**
     * Default Constructor - Initializes the state to Unknown.
     */
    public ClientState() {
        setState(EClientState.Unknown);
    }

    /**
     * Check if this client instance is available.
     *
     * @return - Is Available?
     */
    @JsonIgnore
    public boolean isAvailable() {
        return (getState() == EClientState.Available);
    }

    /**
     * Check if the client is in the expected state.
     *
     * @param expected - Expected State.
     * @throws StateException - If not in expected state error will be thrown.
     */
    public void checkState(EClientState expected) throws StateException {
        if (getState() != expected) {
            throw new StateException(String.format(
                    "Invalid Client State : [expected=%s][current=%s]",
                    expected.name(), getState().name()));
        }
    }

    /**
     * Mark this client instance as Disposed.
     */
    public void dispose() {
        if (getState() == EClientState.Initialized ||
                getState() == EClientState.Available) {
            setState(EClientState.Disposed);
        }
    }
}
