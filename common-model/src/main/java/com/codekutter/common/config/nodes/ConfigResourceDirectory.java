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
 * Date: 24/2/19 12:36 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.config.nodes;

import com.codekutter.common.config.Configuration;
import com.codekutter.common.config.EResourceType;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.File;

/**
 * Configuration resource node for the type directory.
 */
public class ConfigResourceDirectory extends ConfigResourceFile {
    public static final String NODE_NAME = "directory";

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigResourceDirectory() {
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigResourceDirectory(
            Configuration configuration,
            AbstractConfigNode parent) {
        super(configuration, parent);
    }

    /**
     * Override the set resource method to check if the specified file handle is a directory.
     *
     * @param resourceHandle - File handle.
     */
    @Override
    public void setResourceHandle(File resourceHandle) {
        Preconditions.checkArgument(resourceHandle != null);
        Preconditions.checkArgument(resourceHandle.isDirectory());

        super.setResourceHandle(resourceHandle);
    }

    /**
     * Override the set type method, as this resource can only be of type DIRECTORY.
     *
     * @param type - Resource type.
     */
    @Override
    public void setType(EResourceType type) {
        super.setType(EResourceType.DIRECTORY);
    }

    /**
     * Recursive search for files based on the parts and index.
     *
     * @param directory - Current directory node.
     * @param parts     - Path split into parts array.
     * @param index     - Current index in the parts array.
     * @return - File or NULL.
     */
    private File findFile(File directory, String[] parts, int index) {
        Preconditions.checkArgument(directory != null);
        Preconditions.checkArgument(directory.isDirectory());

        String name = parts[index];
        if (directory.getName().compareTo(name) == 0) {
            if (index == parts.length - 1) {
                return directory;
            }
            File[] files = directory.listFiles();
            if (files != null && files.length > 0) {
                String cname = parts[index + 1];
                for (File file : files) {
                    if (file.isDirectory()) {
                        File ret = findFile(file, parts, index + 1);
                        if (ret != null) {
                            return ret;
                        }
                    } else {
                        if (file.getName().compareTo(cname) == 0) {
                            if (index + 1 == parts.length - 1) {
                                return file;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Find a specific file within the directory.
     * <p>
     * The specified path is expected to be relative to the root directory.
     *
     * @param path - Relative Path to search for.
     * @return - File or NULL.
     */
    public File findFile(String path) {
        Preconditions.checkArgument(getResourceHandle() != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));

        String[] parts = path.split("\\/");

        return findFile(getResourceHandle(), parts, 0);
    }
}
