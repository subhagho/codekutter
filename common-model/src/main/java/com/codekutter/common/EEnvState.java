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
 * Date: 10/2/19 5:57 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common;


/**
 * State enum representing a environment state.
 */
public enum EEnvState implements
                      IState<EEnvState> {
    /**
     * Environment State is unknown.
     */
    Unknown,
    /**
     * Environment has been initialized and available.
     */
    Initialized,
    /**
     * Environment has been disposed.
     */
    Disposed,
    /**
     * Environment has error(s).
     */
    Error;


    /**
     * Get the state that represents an error state.
     *
     * @return - Error state.
     */
    @Override
    public EEnvState getErrorState() {
        return Error;
    }
}
