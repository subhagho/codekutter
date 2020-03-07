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
 * Date: 9/2/19 10:12 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;

/**
 * Class represents a service instance of the Configuration Server.
 */
@ConfigPath(path = "zconfig/instance")
public class ZConfigCoreInstance extends ZConfigInstance {

    /**
     * Port this instance is listening on.
     */
    @ConfigValue(name = "port", required = true)
    private int port;


    /**
     * Get the port this service is listening on.
     *
     * @return - Server Port
     */
    public int getPort() {
        return port;
    }

    /**
     * Get the port this service is listening on.
     *
     * @param port - Server Port
     */
    public void setPort(int port) {
        this.port = port;
    }

}
