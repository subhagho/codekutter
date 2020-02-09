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

package com.codekutter.zconfig.common.model.nodes;

import com.codekutter.common.GlobalConstants;
import com.codekutter.common.ValueParseException;
import com.codekutter.zconfig.common.ConfigKeyVault;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.ENodeState;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Class represents a configuration value element. All configuration values are treated as String values
 * and converters provided to get a specific value type.
 * <p>
 * JSON:
 * <pre>
 *     name : value,
 *     ...
 * </pre>
 * <p>
 * XML:
 * <pre>
 *     <name value="value"/>
 *     <param name="name" value="value .../>
 * </pre>
 */
public class ConfigValueNode extends AbstractConfigNode
        implements IConfigValue<String> {

    /**
     * Configuration value element.
     */
    private String value;
    /**
     * Is the data in this node encrypted?
     */
    private boolean encrypted = false;
    /**
     * Value type expected of this value.
     */
    private EValueType valueType = EValueType.STRING;
    /**
     * Enum Type if the value type is Enum.
     */
    @SuppressWarnings("rawtypes")
    private Class<? extends Enum> enumType = null;

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigValueNode() {
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigValueNode(Configuration configuration,
                           AbstractConfigNode parent) {
        super(configuration, parent);
    }

    /**
     * Get the configuration value element.
     *
     * @return - Value.
     */
    @Override
    public String getValue() {
        return value;
    }

    /**
     * Get the decrypted value of this node.
     *
     * @return - Decrypted value.
     */
    @JsonIgnore
    public String getDecryptedValue() {
        if (encrypted && !Strings.isNullOrEmpty(this.value)) {
            try {
                ConfigKeyVault vault = ConfigKeyVault.getInstance();
                if (vault == null) {
                    throw new Exception("Configuration Key Vault not registered.");
                }
                String value = vault.decrypt(this.value, getConfiguration());
                if (Strings.isNullOrEmpty(value)) {
                    throw new Exception(
                            "Error Decrypting Value. NULL value returned.");
                }
                return value;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return value;
    }

    /**
     * Set this config node value to the specified value string.
     *
     * @param value - Value String.
     */
    public void setValue(String value) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        this.value = value;
    }

    /**
     * Is the node value encrypted?
     *
     * @return - Is encrypted?
     */
    public boolean isEncrypted() {
        return encrypted;
    }

    /**
     * Set the node value encryption.
     *
     * @param encrypted - Encrypted?
     */
    public void setEncrypted(boolean encrypted) {
        this.encrypted = encrypted;
    }

    public void setValueType(@Nonnull EValueType type) {
        this.valueType = type;
    }

    public EValueType getValueType() {
        return valueType;
    }

    public void setEnumType(Class<? extends Enum> enumType) {
        this.enumType = enumType;
    }

    public Class<? extends Enum> getEnumType() {
        return enumType;
    }

    /**
     * Get the value parsed as Boolean.
     * <p>
     * Note: If value string is NULL/Empty will return false.
     *
     * @return - Value parsed as boolean.
     */
    @JsonIgnore
    public Boolean getBooleanValue() {
        if (!Strings.isNullOrEmpty(value)) {
            return Boolean.parseBoolean(value);
        }
        return null;
    }

    /**
     * Get the value parsed as Short.
     * <p>
     * Note: If value string is NULL/Empty will return Short.MIN_VALUE.
     *
     * @return - Value parsed as short.
     */
    @JsonIgnore
    public Short getShortValue() {
        if (!Strings.isNullOrEmpty(value)) {
            return Short.parseShort(value);
        }
        return null;
    }

    /**
     * Get the value parsed as Integer.
     * <p>
     * Note: If value string is NULL/Empty will return Integer.MIN_VALUE.
     *
     * @return - Value parsed as integer.
     */
    @JsonIgnore
    public Integer getIntValue() {
        if (!Strings.isNullOrEmpty(value)) {
            return Integer.parseInt(value);
        }
        return null;
    }

    /**
     * Get the value parsed as Long.
     * <p>
     * Note: If value string is NULL/Empty will return Long.MIN_VALUE.
     *
     * @return - Value parsed as long.
     */
    @JsonIgnore
    public Long getLongValue() {
        if (!Strings.isNullOrEmpty(value)) {
            return Long.parseLong(value);
        }
        return null;
    }

    /**
     * Get the value parsed as Float.
     * <p>
     * Note: If value string is NULL/Empty will return Float.MIN_VALUE.
     *
     * @return - Value parsed as float.
     */
    @JsonIgnore
    public Float getFloatValue() {
        if (!Strings.isNullOrEmpty(value)) {
            return Float.parseFloat(value);
        }
        return null;
    }

    /**
     * Get the value parsed as Double.
     * <p>
     * Note: If value string is NULL/Empty will return Double.MIN_VALUE.
     *
     * @return - Value parsed as double.
     */
    @JsonIgnore
    public Double getDoubleValue() {
        if (!Strings.isNullOrEmpty(value)) {
            return Double.parseDouble(value);
        }
        return null;
    }

    public Object getParsedValue() throws ValueParseException {
        Preconditions.checkState(!encrypted);
        if (valueType != EValueType.ENUM) {
            return valueType.parseValue(valueType, valueType.getType(valueType), value);
        } else {
            Preconditions.checkState(enumType != null);
            return valueType.parseValue(valueType, enumType, value);
        }
    }

    /**
     * Get the value parsed as DateTime (Joda). This will use the
     * GlobalConstants#DEFAULT_JODA_DATE_FORMAT for parsing the date.
     *
     * <p>
     * Note: If value string is NULL/Empty will return null.
     *
     * @return - Value parsed as double.
     */
    @JsonIgnore
    public DateTime getDateValue() {
        if (!Strings.isNullOrEmpty(value)) {
            DateTimeFormatter formatter =
                    DateTimeFormat
                            .forPattern(GlobalConstants.DEFAULT_JODA_DATE_FORMAT);
            return DateTime.parse(value, formatter);
        }
        return null;
    }

    /**
     * Get the value parsed as DateTime (Joda). This will use the
     * specified date format for parsing the date.
     *
     * <p>
     * Note: If value string is NULL/Empty will return null.
     *
     * @return - Value parsed as double.
     */
    @JsonIgnore
    public DateTime getDateValue(String format) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(format));
        if (!Strings.isNullOrEmpty(value)) {
            DateTimeFormatter formatter =
                    DateTimeFormat.forPattern(format);
            return DateTime.parse(value, formatter);
        }
        return null;
    }

    /**
     * Get the value parsed as DateTime (Joda). This will use the
     * GlobalConstants#DEFAULT_JODA_DATETIME_FORMAT for parsing the date.
     *
     * <p>
     * Note: If value string is NULL/Empty will return null.
     *
     * @return - Value parsed as double.
     */
    @JsonIgnore
    public DateTime getDateTimeValue() {
        if (!Strings.isNullOrEmpty(value)) {
            DateTimeFormatter formatter =
                    DateTimeFormat
                            .forPattern(
                                    GlobalConstants.DEFAULT_JODA_DATETIME_FORMAT);
            return DateTime.parse(value, formatter);
        }
        return null;
    }

    /**
     * Check if the current path element matches this node name, if so return this instance.
     *
     * @param path  - Tokenized Path array.
     * @param index - Current index in the path array to search for.
     * @return - This instance or NULL.
     */
    @Override
    public AbstractConfigNode find(List<String> path, int index) {
        String key = path.get(index);
        if (getName().compareTo(key) == 0 && (index == path.size() - 1)) {
            return this;
        }
        return null;
    }

    /**
     * Mark the configuration instance has been completely loaded.
     *
     * @throws ConfigurationException
     */
    @Override
    public void loaded() throws ConfigurationException {
        if (getState().hasError()) {
            throw new ConfigurationException(String.format(
                    "Cannot mark as loaded : Object state is in error. [state=%s]",
                    getState().getState().name()));
        }
        updateState(ENodeState.Synced);
    }

    /**
     * Update the node states recursively to the new state.
     *
     * @param state - New state.
     */
    @Override
    public void updateState(ENodeState state) {
        getState().setState(state);
    }

    /**
     * Change the configuration instance this node belongs to.
     * Used for included configurations.
     *
     * @param configuration - Changed configuration.
     */
    @Override
    public void changeConfiguration(Configuration configuration) {
        setConfiguration(configuration);
    }

    /**
     * Validate that this node has been setup correctly.
     *
     * @throws ConfigurationException
     */
    @Override
    public void validate() throws ConfigurationException {
        super.validate();
        if (valueType == EValueType.ENUM) {
            if (enumType == null)
                throw new ConfigurationException(String.format("Enum Type not specified for value. [value type=%s]", valueType.name()));
        }
    }
}
