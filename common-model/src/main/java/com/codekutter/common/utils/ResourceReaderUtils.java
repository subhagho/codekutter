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
 * Date: 17/2/19 8:12 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.Properties;

/**
 * Utility class to read text from a specified resource file.
 */
public class ResourceReaderUtils {
    /**
     * Read the resource content as text.
     *
     * @param source - Source resource.
     * @return - String content.
     * @throws Exception
     */
    public static String readResourceAsText(String source) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(source));
        ClassLoader classLoader =
                ResourceReaderUtils.class.getClassLoader();
        URL url = classLoader.getResource(source);
        if (url != null) {
            File file = new File(url.getFile());
            return new String(Files.readAllBytes(file.toPath()));
        }
        return null;
    }

    /**
     * Read the resource content as Properties.
     *
     * @param source - Source resource.
     * @return - Read Properties.
     * @throws Exception
     */
    public static Properties readProperties(String source) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(source));
        ClassLoader classLoader =
                ResourceReaderUtils.class
                        .getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(source);
        if (inputStream != null) {
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        }
        return null;
    }
}
