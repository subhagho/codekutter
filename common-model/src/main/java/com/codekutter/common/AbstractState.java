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
 * Date: 1/1/19 9:22 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common;


/**
 * Abstract base class used to define state of objects/instances.
 *
 * @param <T> - State enum to be managed.
 */
public abstract class AbstractState<T extends IState<T>> {
    /**
     * State of the instance type.
     */
    private T state;
    /**
     * Error handle in case of error state.
     */
    protected Throwable error;

    /**
     * Get the current state.
     *
     * @return - Current state
     */
    public T getState() {
        return state;
    }

    /**
     * Update the state to the specified state.
     *
     * @param state - State to set.
     */
    public void setState(T state) {
        this.state = state;
    }

    /**
     * Get the exception associated with this state. Exception handle will be returned
     * only if the current state is error.
     *
     * @return - Exception handle, null if state is not error.
     */
    public Throwable getError() {
        if (this.state == state.getErrorState())
            return error;
        return null;
    }

    /**
     * Set the exception handle for this state instance. Will also set the current state to error state.
     *
     * @param error - Exception handle.
     */
    public void setError(Throwable error) {
        this.state = state.getErrorState();
        this.error = error;
    }

    /**
     * Check if the state is in Error State.
     *
     * @return - In error state?
     */
    public boolean hasError() {
        if (state != null && state == state.getErrorState()) {
            return true;
        }
        return false;
    }
}
