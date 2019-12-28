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
 * Date: 1/1/19 9:24 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common;

/**
 * Interface to be implemented for enums that are used to represent object/process states.
 */
public interface IState<T> {
    /**
     * Get the state that represents an error state.
     *
     * @return - Error state.
     */
    public T getErrorState();
}
