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
 * Date: 2/2/19 10:36 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.model;

import com.codekutter.common.GlobalConstants;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * Enum specifies the type of defined readers.
 */
public enum EReaderType {
    /**
     * Reader reads from a local file path.
     */
    File,
    /**
     * Reader reads from a specified HTTP.
     */
    HTTP,
    /**
     * Reader reads from a specified HTTPS.
     */
    HTTPS,
    /**
     * Reader reads from a specified FTP location.
     */
    FTP,
    /**
     * Reader reads from a specified SFTP location.
     */
    SFTP;

    /**
     * Parse the reader type from the specified string value.
     *
     * @param type - String value.
     * @return - Parsed Reader Type.
     */
    public static EReaderType parse(@Nonnull String type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(type));
        if (type.compareToIgnoreCase(File.name()) == 0) {
            return File;
        } else if (type.compareToIgnoreCase(HTTP.name()) == 0) {
            return HTTP;
        } else if (type.compareToIgnoreCase(HTTPS.name()) == 0) {
            return HTTPS;
        }
        return null;
    }

    /**
     * Get the configuration reader type based on the URI protocol.
     *
     * @param uri - Specified URI
     * @return - Reader Type or NULL.
     */
    public static EReaderType parseFromUri(@Nonnull URI uri) {
        Preconditions.checkArgument(uri != null);
        String scheme = uri.getScheme();
        if (!Strings.isNullOrEmpty(scheme)) {
            if (scheme.compareToIgnoreCase(GlobalConstants.URI_SCHEME_FILE) == 0) {
                return File;
            } else if (
                    scheme.compareToIgnoreCase(GlobalConstants.URI_SCHEME_HTTP) ==
                            0) {
                return HTTP;
            } else if (
                    scheme.compareToIgnoreCase(GlobalConstants.URI_SCHEME_HTTPS) ==
                            0) {
                return HTTPS;
            } else if (scheme.compareToIgnoreCase(GlobalConstants.URI_SCHEME_FTP) ==
                    0) {
                return FTP;
            } else if (
                    scheme.compareToIgnoreCase(GlobalConstants.URI_SCHEME_SFTP) ==
                            0) {
                return SFTP;
            }
        }
        return null;
    }

    /**
     * Get the URI Scheme for this Reader Type.
     *
     * @param type - Reader Type.
     * @return - URI Scheme
     */
    public static String getURIScheme(EReaderType type) {
        switch (type) {
            case HTTP:
                return GlobalConstants.URI_SCHEME_HTTP;
            case HTTPS:
                return GlobalConstants.URI_SCHEME_HTTPS;
            case File:
                return GlobalConstants.URI_SCHEME_FILE;
            case FTP:
                return GlobalConstants.URI_SCHEME_FTP;
            case SFTP:
                return GlobalConstants.URI_SCHEME_SFTP;
            default:
                return null;
        }
    }
}
