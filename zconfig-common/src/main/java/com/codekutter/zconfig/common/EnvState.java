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
 * Date: 10/2/19 6:00 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.AbstractState;
import com.codekutter.common.StateException;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Class represents a environment state instance.
 */
public class EnvState extends AbstractState<EEnvState> {

    /**
     * Default Constructor - Initialize state to Unknown
     */
    public EnvState() {
        setState(EEnvState.Unknown);
    }

    /**
     * Check if this service instance is in a Initialized state.
     *
     * @return - Is Initialized?
     */
    @JsonIgnore
    public boolean isInitialized() {
        return (getState() == EEnvState.Initialized);
    }

    /**
     * Mark this instance state as Disposed.
     */
    public void dispose() {
        if (getState() == EEnvState.Initialized) {
            setState(EEnvState.Disposed);
        }
    }

    /**
     * Check if the current state is in the expected state.
     *
     * @param expected - Expected state.
     * @throws StateException - Will be raised if state doesn't match with expected.
     */
    public void checkState(EEnvState expected) throws StateException {
        if (getState() != expected) {
            throw new StateException(
                    String.format("Invalid State Error : [expected=%s][current=%s]",
                                  expected.name(), getState().name()));
        }
    }
}
