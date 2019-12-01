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
 * Date: 24/2/19 11:53 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.config;

import com.codekutter.common.config.annotations.ConfigPath;
import com.codekutter.common.config.annotations.ConfigValue;
import com.codekutter.common.utils.IOUtils;
import com.google.common.base.Strings;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Settings definition for parsing/writing configurations.
 */
@ConfigPath(path = "*.configurationSettings")
public class ConfigurationSettings {
    /**
     * Enum to specify startup action options.
     */
    public static enum EStartupOptions {
        /**
         * Perform action on StartUp
         */
        OnStartUp,
        /**
         * Perform action on demand.
         */
        OnDemand
    }

    /**
     * Enum to specify shutdown action options.
     */
    public static enum EShutdownOptions {
        /**
         * Clear data on shutdown.
         */
        ClearOnShutdown,
        /**
         * Don't clear data on shutdown, will be reused on next startup.
         */
        ReuseData
    }

    /**
     * Wildcard for node search.
     */
    public static final String NODE_SEARCH_WILDCARD = "*";
    public static final String NODE_SEARCH_RECURSIVE_WILDCARD = "**";
    public static final String NODE_PARENT_TERM = "..";
    public static final String NODE_SEARCH_SEPERATOR = "/";

    private static final String DEFAULT_PROPS_NAME = "properties";
    private static final String DEFAULT_ATTR_NAME = "@";
    private static final String DEFAULT_PARAMS_NAME = "parameters";
    public static final String ARRAY_INDEX_REGEX = "^(\\w*)\\[(\\d*)\\]$";
    public static final String PARAM_NODE_CHAR = "#";
    public static final String PROP_NODE_CHAR = "$";
    public static final String ATTR_NODE_CHAR = "@";
    public static final Pattern INDEX_PATTERN = Pattern.compile(ARRAY_INDEX_REGEX);

    @ConfigValue(name = "propertiesTag")
    private String propertiesNodeName = DEFAULT_PROPS_NAME;
    @ConfigValue(name = "parametersTag")
    private String parametersNodeName = DEFAULT_PARAMS_NAME;
    @ConfigValue(name = "attributesTag")
    private String attributesNodeName = DEFAULT_ATTR_NAME;
    @ConfigValue(name = "tempDir")
    private String tempDirectory = IOUtils.getTempDirectory();
    @ConfigValue(name = "downloadOption")
    private EStartupOptions downloadRemoteFiles = EStartupOptions.OnStartUp;
    @ConfigValue(name = "shutdownOption")
    private EShutdownOptions clearTempFolder = EShutdownOptions.ReuseData;

    /**
     * Get the Properties Node name.
     *
     * @return - Properties Node name.
     */
    public String getPropertiesNodeName() {
        return propertiesNodeName;
    }

    /**
     * Set the Properties Node name.
     *
     * @param propertiesNodeName - Properties Node name.
     */
    public void setPropertiesNodeName(String propertiesNodeName) {
        this.propertiesNodeName = propertiesNodeName;
    }

    /**
     * Get the Parameters Node name.
     *
     * @return - Parameters Node name.
     */
    public String getParametersNodeName() {
        return parametersNodeName;
    }

    /**
     * Set the Parameters Node name.
     *
     * @param parametersNodeName - Parameters Node name.
     */
    public void setParametersNodeName(String parametersNodeName) {
        this.parametersNodeName = parametersNodeName;
    }

    /**
     * Get the Attributes Node name.
     *
     * @return - Attributes Node name.
     */
    public String getAttributesNodeName() {
        return attributesNodeName;
    }

    /**
     * Set the Attributes Node name.
     *
     * @param attributesNodeName - Attributes Node name.
     */
    public void setAttributesNodeName(String attributesNodeName) {
        this.attributesNodeName = attributesNodeName;
    }

    /**
     * Get the temp directory for this configuration.
     *
     * @return - Temp directory.
     */
    public String getTempDirectory() {
        return tempDirectory;
    }

    /**
     * Set the temp directory for this configuration.
     *
     * @param tempDirectory - Temp directory.
     */
    public void setTempDirectory(String tempDirectory) {
        this.tempDirectory = tempDirectory;
    }

    /**
     * Get option to download remote files.
     *
     * @return - Download Option.
     */
    public EStartupOptions getDownloadRemoteFiles() {
        return downloadRemoteFiles;
    }

    /**
     * Set option to download remote files.
     *
     * @param downloadRemoteFiles - Download Option.
     */
    public void setDownloadRemoteFiles(
            EStartupOptions downloadRemoteFiles) {
        this.downloadRemoteFiles = downloadRemoteFiles;
    }

    /**
     * Get option to clear temporary files.
     *
     * @return - Clear temporary files?
     */
    public EShutdownOptions getClearTempFolder() {
        return clearTempFolder;
    }

    /**
     * Set option to clear temporary files.
     *
     * @param clearTempFolder - Clear temporary files?
     */
    public void setClearTempFolder(
            EShutdownOptions clearTempFolder) {
        this.clearTempFolder = clearTempFolder;
    }

    /**
     * Get the temp directory to store configuration temporary files.
     * Will attempt to create folder(s) if required.
     *
     * @param subdir - Sub Directory under the TEMP folder.
     * @return - Path to temp directory.
     * @throws IOException
     */
    public String getConfigTempFolder(String subdir) throws IOException {
        String dir = tempDirectory;
        if (!Strings.isNullOrEmpty(subdir)) {
            dir = String.format("%s/%s", dir, subdir);
        }
        File df = new File(dir);
        if (!df.exists()) {
            if (!df.mkdirs()) {
                throw new IOException(
                        String.format("Error creating temp directory : [path=%s]",
                                      df.getAbsolutePath()));
            }
        }
        return df.getAbsolutePath();
    }

    /**
     * Check if this name is a wildcard.
     *
     * @param name - Node name.
     * @return - Is Wildcard?
     */
    public static boolean isWildcard(String name) {
        if (!Strings.isNullOrEmpty(name)) {
            return (NODE_SEARCH_WILDCARD.compareTo(name.trim()) == 0);
        }
        return false;
    }

    /**
     * Check if this name is a wildcard.
     *
     * @param name - Node name.
     * @return - Is Wildcard?
     */
    public static boolean isRecursiveWildcard(String name) {
        if (!Strings.isNullOrEmpty(name)) {
            return (NODE_SEARCH_RECURSIVE_WILDCARD.compareTo(name.trim()) == 0);
        }
        return false;
    }
}
