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

package com.codekutter.zconfig.common.model.nodes;

import com.codekutter.common.ValueParseException;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

/**
 * Enum to specify the value type for a configuration value node.
 */
public enum EValueType {
    /**
     * Type - String
     */
    STRING,
    /**
     * Type - Boolean
     */
    BOOL,
    /**
     * Type - Char
     */
    CHAR,
    /**
     * Type - Short
     */
    SHORT,
    /**
     * Type - Integer
     */
    INT,
    /**
     * Type - Long
     */
    LONG,
    /**
     * Type - Float
     */
    FLOAT,
    /**
     * Type - Double
     */
    DOUBLE,
    /**
     * Type - Class
     */
    CLASS,
    /**
     * Type - Enum
     */
    ENUM;

    public Class<?> getType(@Nonnull EValueType type) {
        switch (type) {
            case BOOL:
                return Boolean.class;
            case INT:
                return Integer.class;
            case CHAR:
                return Character.class;
            case ENUM:
                return Enum.class;
            case LONG:
                return Long.class;
            case CLASS:
                return Class.class;
            case FLOAT:
                return Float.class;
            case SHORT:
                return Short.class;
            case DOUBLE:
                return Double.class;
            case STRING:
                return String.class;
        }
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object parseValue(@Nonnull EValueType type,
                             @Nonnull Class<?> cType,
                             @Nonnull String value) throws ValueParseException {
        if (Strings.isNullOrEmpty(value)) {
            try {
                switch (type) {
                    case STRING:
                        return value;
                    case DOUBLE:
                        return Double.parseDouble(value);
                    case SHORT:
                        return Short.parseShort(value);
                    case FLOAT:
                        return Float.parseFloat(value);
                    case CLASS:
                        return Class.forName(value);
                    case LONG:
                        return Long.parseLong(value);
                    case ENUM:
                        return Enum.valueOf((Class<? extends Enum>) cType, value);
                    case CHAR:
                        return value.charAt(0);
                    case INT:
                        return Integer.parseInt(value);
                    case BOOL:
                        return Boolean.parseBoolean(value);
                }
            } catch (Throwable t) {
                throw new ValueParseException(t);
            }
        }
        return null;
    }
}
