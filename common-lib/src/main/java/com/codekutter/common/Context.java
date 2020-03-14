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
 * Date: 13/2/19 5:54 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Context handle to be used for passing parameters.
 */
public class Context {
    /**
     * Context data Map.
     */
    private Map<String, Object> params = new HashMap<>();

    /**
     * Default Empty constructor.
     */
    public Context() {

    }

    /**
     * Copy Constructor for initializing the current Context.
     *
     * @param source - Source context to initialize from.
     */
    public Context(@Nonnull Context source) {
        if (!source.params.isEmpty()) {
            for (String key : source.params.keySet()) {
                Object value = source.params.get(key);
                params.put(key, value);
            }
        }
    }

    /**
     * Get the values in this context.
     *
     * @return - Context values.
     */
    public Collection<Object> getValues() {
        return params.values();
    }

    /**
     * Get the keys in this context.
     *
     * @return - Context keys.
     */
    public Collection<String> getKeys() {
        return params.keySet();
    }

    /**
     * Add a param (key/value).
     *
     * @param key   - Param Key.
     * @param value - Param Value.
     */
    public void setParam(@Nonnull String key, Object value) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        params.put(key, value);
    }

    /**
     * Get the param value for the specified key.
     *
     * @param key - Param Key.
     * @return - Param Value.
     */
    public Object getParam(@Nonnull String key) {
        return params.get(key);
    }

    /**
     * Remove the param value for the specified key.
     *
     * @param key - Param Key to remove.
     * @return - Is Removed?
     */
    public boolean removeParam(@Nonnull String key) {
        return (params.remove(key) != null);
    }

    /**
     * Get the String value for the specified Param Key.
     *
     * @param key - Param Key.
     * @return - String Value.
     */
    public String getStringParam(@Nonnull String key) {
        Object value = getParam(key);
        if (value != null) {
            return String.valueOf(value);
        }
        return null;
    }

    /**
     * Get the Boolean value for the specified Param Key.
     *
     * @param key - Param Key.
     * @return - Boolean Value.
     */
    public Boolean getBoolParam(@Nonnull String key) {
        Object value = getParam(key);
        if (value != null) {
            if (value instanceof Boolean) {
                return (Boolean) value;
            } else if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            }
        }
        return false;
    }

    /**
     * Get the Short value for the specified Param Key.
     *
     * @param key - Param Key.
     * @return - Short Value.
     */
    public Short getShortParam(@Nonnull String key) {
        Object value = getParam(key);
        if (value != null) {
            if (value instanceof Short) {
                return (Short) value;
            } else if (value instanceof String) {
                return Short.parseShort((String) value);
            }
        }
        return null;
    }

    /**
     * Get the Integer value for the specified Param Key.
     *
     * @param key - Param Key.
     * @return - Integer Value.
     */
    public Integer getIntParam(@Nonnull String key) {
        Object value = getParam(key);
        if (value != null) {
            if (value instanceof Integer) {
                return (Integer) value;
            } else if (value instanceof String) {
                return Integer.parseInt((String) value);
            }
        }
        return null;
    }

    /**
     * Get the Long value for the specified Param Key.
     *
     * @param key - Param Key.
     * @return - Long Value.
     */
    public Long getLongParam(@Nonnull String key) {
        Object value = getParam(key);
        if (value != null) {
            if (value instanceof Long) {
                return (Long) value;
            } else if (value instanceof String) {
                return Long.parseLong((String) value);
            }
        }
        return null;
    }

    /**
     * Get the Float value for the specified Param Key.
     *
     * @param key - Param Key.
     * @return - Float Value.
     */
    public Float getFloatParam(@Nonnull String key) {
        Object value = getParam(key);
        if (value != null) {
            if (value instanceof Float) {
                return (Float) value;
            } else if (value instanceof String) {
                return Float.parseFloat((String) value);
            }
        }
        return null;
    }

    /**
     * Get the Double value for the specified Param Key.
     *
     * @param key - Param Key.
     * @return - Double Value.
     */
    public Double getDoubleParam(@Nonnull String key) {
        Object value = getParam(key);
        if (value != null) {
            if (value instanceof Double) {
                return (Double) value;
            } else if (value instanceof String) {
                return Double.parseDouble((String) value);
            }
        }
        return null;
    }

    /**
     * Get the Double value for the specified Param Key.
     * If value is String, use the passed Date/Time Format.
     *
     * @param key    - Param Key.
     * @param format - Date/Time format.
     * @return - DateTime Value.
     */
    public DateTime getDateParam(@Nonnull String key, String format) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(format));
        Object value = getParam(key);
        if (value != null) {
            if (value instanceof DateTime) {
                return (DateTime) value;
            } else if ((value instanceof String) &&
                    !Strings.isNullOrEmpty(format)) {
                DateTimeFormatter fmt = DateTimeFormat.forPattern(format);
                return fmt.parseDateTime(key);
            }
        }
        return null;
    }

    /**
     * Get the Double value for the specified Param Key.
     * If value is String, use the Default Date/Time Format.
     *
     * @param key - Param Key.
     * @return - DateTime Value.
     */
    public DateTime getDateParam(@Nonnull String key) {
        return getDateParam(key, GlobalConstants.DEFAULT_JODA_DATETIME_FORMAT);
    }
}
