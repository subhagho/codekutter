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
 * Date: 4/1/19 5:43 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.writers;

import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.Configuration;

/**
 * Abstract base class for defining configuration writers.
 * Writers will serialize a configuration instance if to selected serialization format.
 */
public abstract class AbstractConfigWriter {

    /**
     * Write this instance of the configuration to the specified output location.
     *
     * @param path - Output location to write to.
     * @return - Return the path of the output file created.
     * @throws ConfigurationException
     */
    public abstract String write(Configuration configuration, String path) throws ConfigurationException;
}
