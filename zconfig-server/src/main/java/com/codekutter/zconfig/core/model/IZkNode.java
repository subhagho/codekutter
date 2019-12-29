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
 * Date: 15/2/19 8:33 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Interface to be implemented by all ZooKeeper node elements.
 */
public interface IZkNode {
    /**
     * Get the name of this node.
     *
     * @return - Node name
     */
    public String getName();

    /**
     * Get the path name of this node.
     *
     * @return - Path node name.
     */
    @JsonIgnore
    public String getPath();

    /**
     * Get the absolute path of this node element.
     *
     * @return - Absolute Path.
     */
    @JsonIgnore
    public String getAbsolutePath();
}
