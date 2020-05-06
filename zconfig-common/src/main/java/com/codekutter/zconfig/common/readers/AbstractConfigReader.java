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
 * Date: 2/2/19 9:50 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.readers;


import com.codekutter.zconfig.common.ConfigurationException;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.InputStream;

/**
 * Abstract base class for reading configuration data.
 */
public abstract class AbstractConfigReader implements Closeable {
    /**
     * State of this reader.
     */
    protected ReaderState state = new ReaderState();

    /**
     * Get the state of this reader instance.
     *
     * @return - Reader state.
     */
    public ReaderState getState() {
        return state;
    }

    /**
     * Set the state of this reader.
     *
     * @param state - Reader state.
     */
    public void setState(ReaderState state) {
        this.state = state;
    }

    /**
     * Check if this reader is opened.
     *
     * @return - Is opened?
     */
    public boolean isOpen() {
        return state.isOpen();
    }

    /**
     * Check if this reader is closed.
     *
     * @return - Is closed?
     */
    public boolean isClosed() {
        return (state.getState() == EReaderState.Closed);
    }

    /**
     * Open this configuration reader instance.
     *
     * @throws ConfigurationException
     */
    public abstract void open() throws ConfigurationException;

    /**
     * Get the buffered input stream associated with this reader.
     *
     * @return - Input stream.
     * @throws ConfigurationException
     */
    public abstract BufferedReader getBufferedStream()
            throws ConfigurationException;

    /**
     * Get the  input stream associated with this reader.
     *
     * @return - Input stream.
     * @throws ConfigurationException
     */
    public abstract InputStream getInputStream() throws ConfigurationException;

    /**
     * Close this configuration reader instance.
     */
    public abstract void close();
}
