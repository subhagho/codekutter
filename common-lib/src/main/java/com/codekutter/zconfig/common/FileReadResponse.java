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
 * Date: 2/1/19 8:46 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

/**
 * Response handle for reading byte data from a file.
 */
public class FileReadResponse {
    /**
     * Absolute file path of the file read from.
     */
    private String filename;
    /**
     * Start position where data was read from.
     */
    private long position;
    /**
     * Number of bytes read.
     */
    private int readBytes;
    /**
     * File data as bytes.
     */
    private byte[] data;

    /**
     * Default constructor - with filename and start position.
     *
     * @param filename - File path of this file.
     * @param position - Start position to read from.
     */
    public FileReadResponse(String filename, long position) {
        this.filename = filename;
        this.position = position;
    }

    /**
     * Get the file path.
     *
     * @return - File path.
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Get the start position of the read.
     *
     * @return - Start position.
     */
    public long getPosition() {
        return position;
    }

    /**
     * Get the number of bytes read.
     *
     * @return - Number of bytes read.
     */
    public int getReadBytes() {
        return readBytes;
    }

    /**
     * Set the number of bytes read.
     *
     * @param readBytes - Number of bytes read.
     */
    public void setReadBytes(int readBytes) {
        this.readBytes = readBytes;
    }

    /**
     * Get the byte data.
     *
     * @return - Byte data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Set the byte data.
     *
     * @param data - Byte data
     */
    public void setData(byte[] data) {
        this.data = data;
    }
}
