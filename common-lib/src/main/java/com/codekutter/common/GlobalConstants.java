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
 * Date: 1/1/19 9:06 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;

/**
 * Class defines static global constants.
 */
public class GlobalConstants {
    /**
     * Default Joda date format to parse/print dates.
     */
    public static final String DEFAULT_JODA_DATE_FORMAT = "M.d.y";
    /**
     * Default Joda data/time format to parse/print date/time.
     */
    public static final String DEFAULT_JODA_DATETIME_FORMAT =
            String.format("%s H:m:s", DEFAULT_JODA_DATE_FORMAT);

    /**
     * Default Joda date format to parse/print dates.
     */
    public static final String DEFAULT_DATE_FORMAT = "MM.dd.yyyy";
    /**
     * Default Joda data/time format to parse/print date/time.
     */
    public static final String DEFAULT_DATETIME_FORMAT =
            String.format("%s HH:mm:ss", DEFAULT_DATE_FORMAT);

    /**
     * ConfigParam name for passing a configuration node.
     */
    public static final String DEFAULT_CONFIG_PARAM_NAME = "config";

    /**
     * URI Scheme constant for file.
     */
    public static final String URI_SCHEME_FILE = "file";
    /**
     * URI Scheme constant for HTTP.
     */
    public static final String URI_SCHEME_HTTP = "http";
    /**
     * URI Scheme constant for HTTPS.
     */
    public static final String URI_SCHEME_HTTPS = "https";
    /**
     * URI Scheme constant for FTP.
     */
    public static final String URI_SCHEME_FTP = "ftp";
    /**
     * URI Scheme constant for SFTP.
     */
    public static final String URI_SCHEME_SFTP = "sftp";

    private static String OS = System.getProperty("os.name").toLowerCase();

    /**
     * Is this a windows OS?
     *
     * @return - Windows OS?
     */
    public static boolean isWindows() {
        return (OS.contains("win"));
    }

    /**
     * Is this a Mac OS?
     *
     * @return - Mac OS?
     */
    public static boolean isMac() {
        return (OS.contains("mac"));
    }

    /**
     * Is this a *NIX OS?
     *
     * @return - (U)NIX OS?
     */
    public static boolean isUnix() {
        return (OS.contains("nix") || OS.contains("nux") || OS.indexOf("aix") > 0);
    }

    /**
     * Is this a Solaris OS?
     *
     * @return - Solaris OS?
     */
    public static boolean isSolaris() {
        return (OS.contains("sunos"));
    }

    /**
     * Get a new instance of the JSON Object mapper.
     *
     * @return - JSON Object mapper.
     */
    public static ObjectMapper getJsonMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        return mapper;
    }
}
