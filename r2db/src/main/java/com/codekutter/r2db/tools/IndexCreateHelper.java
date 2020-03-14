/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.codekutter.r2db.tools;

import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.r2db.driver.impl.annotations.Indexed;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Date;

public class IndexCreateHelper {
    public static String analysisSetting(String analyzer, Indexed indexed) {
        JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
        ObjectNode root = nodeFactory.objectNode();
        root
                .putObject("analysis")
                .putObject("analyzer")
                .putObject(analyzer)
                .put("type", "custom")
                .put("tokenizer", indexed.tokenizer())
                .put("filter", "lowercase");
         return root.toPrettyString();
    }

    public static String parseFieldType(Field field) {
        Class<?> type = field.getType();
        if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            return Boolean.class.getName().toLowerCase();
        } else if (type.equals(short.class) || type.equals(Short.class)) {
            return Short.class.getName().toLowerCase();
        } else if (type.equals(int.class) || type.equals(Integer.class)) {
            return Integer.class.getName().toLowerCase();
        } else if (type.equals(long.class) || type.equals(Long.class)) {
            return Long.class.getName().toLowerCase();
        } else if (type.equals(Date.class)) {
            return Date.class.getName().toLowerCase();
        } else if (type.equals(Timestamp.class)) {
            return Long.class.getName().toLowerCase();
        } else if (type.equals(String.class)) {
            return "text";
        } else if (type.equals(byte.class) || type.equals(Byte.class)) {
            return Byte.class.getName().toLowerCase();
        } else if (type.equals(float.class) || type.equals(Float.class)) {
            return Float.class.getName().toLowerCase();
        } else if (type.equals(double.class) || type.equals(Double.class)) {
            return Double.class.getName().toLowerCase();
        } 
        return null;
    }
}
