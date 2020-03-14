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
 * Date: 24/1/19 12:17 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.model.EReaderType;
import com.codekutter.zconfig.common.parsers.AbstractConfigParser;
import com.codekutter.zconfig.common.parsers.JSONConfigParser;
import com.codekutter.zconfig.common.parsers.XMLConfigParser;
import com.codekutter.zconfig.common.readers.AbstractConfigReader;
import com.codekutter.zconfig.common.readers.ConfigFileReader;
import com.codekutter.zconfig.common.readers.ConfigURLReader;
import com.codekutter.zconfig.common.writers.AbstractConfigWriter;
import com.codekutter.zconfig.common.writers.JSONFileConfigWriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.io.FilenameUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;

/**
 * Factory class to provide config parser and config reader instances.
 */
public class ConfigProviderFactory {
    /**
     * Method will try to get the configuration parser based on the extension of the
     * specified configuration file name.
     *
     * @param filename - Configuration filename.
     * @return - Configuration Parser instance.
     */
    public static final AbstractConfigParser parser(@Nonnull String filename) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(filename));

        String ext = FilenameUtils.getExtension(filename);
        if (!Strings.isNullOrEmpty(ext)) {
            EConfigType type = EConfigType.parse(ext);
            if (type != null) {
                return parser(type);
            }
        }
        return null;
    }

    /**
     * Get a new configuration parser instance of the parser for the specified configuration type.
     *
     * @param type - Configuration type.
     * @return - Parser instance.
     */
    public static final AbstractConfigParser parser(EConfigType type) {
        switch (type) {
            case JSON:
                return new JSONConfigParser();
            case XML:
                return new XMLConfigParser();
            default:
                return null;
        }
    }

    /**
     * Get a new configuration writer instance of the parser for the specified configuration type.
     *
     * @param type - Configuration type.
     * @return - Writer instance.
     */
    public static final AbstractConfigWriter writer(EConfigType type) {
        switch (type) {
            case JSON:
                return new JSONFileConfigWriter();
            case XML:
                return null;
            default:
                return null;
        }
    }

    /**
     * Get a new instance of a Configuration reader. Type of reader is determined based on the
     * SCHEME of the URI.
     * SCHEME = "http", TYPE = HTTP
     * SCHEME = "file", type = File
     *
     * @param uri - URI to get the input from.
     * @return - Configuration Reader instance.
     * @throws ConfigurationException
     */
    public static final AbstractConfigReader reader(URI uri)
            throws ConfigurationException {
        Preconditions.checkArgument(uri != null);
        EReaderType type = EReaderType.parseFromUri(uri);
        try {
            if (type != null) {
                if (type == EReaderType.HTTP || type == EReaderType.HTTPS) {
                    URL url = uri.toURL();
                    return new ConfigURLReader(url);
                } else if (type == EReaderType.File) {
                    File file = Paths.get(uri).toFile();
                    return new ConfigFileReader(file);
                }
            }
            return null;
        } catch (MalformedURLException e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Enum to define the configuration type.
     */
    public static enum EConfigType {
        /**
         * JSON configuration file.
         */
        JSON,
        /**
         * XML configuration file.
         */
        XML;

        /**
         * Get the config type based on passed string.
         *
         * @param value - String value to parse.
         * @return - Config Type or NULL.
         */
        public static EConfigType parse(String value) {
            if (!Strings.isNullOrEmpty(value)) {
                value = value.trim().toUpperCase();
                if (value.compareTo(JSON.name()) == 0) {
                    return JSON;
                } else if (value.compareTo(XML.name()) == 0) {
                    return XML;
                }
            }
            return null;
        }
    }
}
