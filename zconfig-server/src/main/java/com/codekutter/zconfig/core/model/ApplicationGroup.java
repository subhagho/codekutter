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
 * Date: 13/2/19 11:07 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core.model;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.StringKey;
import com.codekutter.common.model.ValidationExceptions;
import com.codekutter.common.utils.IUniqueIDGenerator;
import com.codekutter.zconfig.core.utils.EntityUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * Class represent a logical grouping of applications where the
 * configurations/access/updates are managed together.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
              property = "@class")
public class ApplicationGroup extends BaseEntity<StringKey, ApplicationGroup>
        implements IZkNode {

    /**
     * Name of this application group.
     */
    private String name;
    /**
     * Description of this application group.
     */
    private String description;
    /**
     * Channel name to publish updates to.
     */
    private String channelName;
    /**
     * Application Group properties.
     */
    private Map<String, String> properties;

    /**
     * Get the Application Group name.
     *
     * @return - Application Group name.
     */
    public String getName() {
        return name;
    }

    /**
     * Set the Application Group name. Application group names
     * are expected to be unique per instance.
     *
     * @param name - Application Group name.
     */
    public void setName(@Nonnull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        this.name = name;
    }

    /**
     * Get the description of this Application Group.
     *
     * @return - Application Group description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Set the description of this Application Group.
     *
     * @param description - Application Group description
     */
    public void setDescription(@Nonnull String description) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(description));

        this.description = description;
    }

    /**
     * Get the channel name to publish updates to.
     *
     * @return - Update channel name.
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Set the channel name to publish updates to.
     *
     * @param channelName - Update channel name.
     */
    public void setChannelName(@Nonnull String channelName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(channelName));
        this.channelName = channelName;
    }

    /**
     * Get the application group properties.
     *
     * @return - Application Group properties.
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Set the application group properties.
     *
     * @param properties - Application Group properties.
     */
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * Get the property value for the specified key.
     *
     * @param key - Property Key.
     * @return - Property Value, if found else NULL.
     */
    public String getProperty(String key) {
        if (properties != null) {
            return properties.get(key);
        }
        return null;
    }

    /**
     * Add/Update a property value.
     *
     * @param key   - Property Key.
     * @param value - Property Value
     * @return - Property Value
     */
    public String addProperty(@Nonnull String key, @Nonnull String value) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(key, value);
        return value;
    }

    /**
     * Remove the specified property by key.
     *
     * @param key - Property key.
     * @return - Is removed?
     */
    public boolean removeProperty(String key) {
        if (properties != null) {
            return !Strings.isNullOrEmpty(properties.remove(key));
        }
        return false;
    }

    /**
     * Compare this entity instance's key with the passed source.
     *
     * @param source - Source instance to compare with.
     * @return - (<0 key < source.key) (0 key == source.key) (>0 key > source.key)
     */
    @Override
    public int compareKey(PersistedEntity<StringKey, ApplicationGroup> source) {
        return getId().compareTo(source.getId());
    }

    /**
     * Get the computed hash code for this entity instance.
     *
     * @return - Hash Code.
     */
    @Override
    public int getHashCode() {
        return EntityUtils.getStringHashCode(getId().getKey());
    }

    /**
     * Get the path name of this node.
     *
     * @return - Path node name.
     */
    @Override
    @JsonIgnore
    public String getPath() {
        return name;
    }

    /**
     * Get the absolute path of this node element.
     *
     * @return - Absolute Path.
     */
    @Override
    @JsonIgnore
    public String getAbsolutePath() {
        return String.format("/%s", getPath());
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(StringKey key) {
        return getKey().compareTo(key);
    }

    /**
     * Copy the changes from the specified source entity
     * to this instance.
     * <p>
     * All properties other than the Key will be copied.
     * Copy Type:
     * Primitive - Copy
     * String - Copy
     * Enum - Copy
     * Nested Entity - Copy Recursive
     * Other Objects - Copy Reference.
     *
     * @param source  - Source instance to Copy from.
     * @param context - Execution context.
     * @return - Copied Entity instance.
     * @throws CopyException
     */
    @Override
    public IEntity<StringKey> copyChanges(IEntity<StringKey> source, Context context) throws CopyException {
        try {
            return EntityUtils.copyChanges(source, this);
        } catch (EntityException e) {
            throw new CopyException(e);
        }
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<StringKey> clone(Context context) throws CopyException {
        return null;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {

    }
}
