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
 * Date: 1/1/19 10:53 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser to get and set regex replacements for property place holders.
 */
public class VariableRegexParser {
    private static final String VAR_REGEX = "\\$\\{(.*?)\\}";
    private static final Pattern VAR_PATTERN = Pattern.compile(VAR_REGEX);

    /**
     * Check if the input string has any variables defined.
     *
     * @param value - Input string.
     * @return - Has variable?
     */
    public static boolean hasVariable(String value) {
        Matcher matcher = VAR_PATTERN.matcher(value);
        if (matcher.find()) {
            return true;
        }
        return false;
    }

    /**
     * Get the list of variables defined in the input string.
     *
     * @param value - Input string.
     * @return - List of variables or NULL if none defined.
     */
    public static List<String> getVariables(String value) {
        Matcher matcher = VAR_PATTERN.matcher(value);
        List<String> vars = new ArrayList<>();
        while (matcher.find()) {
            String var = matcher.group(1);
            vars.add(var);
        }
        if (vars.size() > 0)
            return vars;

        return null;
    }
}
