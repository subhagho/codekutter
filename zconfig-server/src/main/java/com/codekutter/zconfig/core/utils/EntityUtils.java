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
 * Date: 13/2/19 5:04 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.core.utils;

import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.IKey;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.core.model.EntityException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;

/**
 * Utility methods for use with entities.
 */
public class EntityUtils {
    private static final String FIELD_ID = "id";

    /**
     * Generate a hash code based on the passed string value.
     *
     * @param value - Generated Hash Code.
     * @return - Hash Code.
     */
    public static int getStringHashCode(String value) {
        final int prime = 31;
        return (prime * (Strings.isNullOrEmpty(value) ? 0 : value.hashCode()));
    }

    /**
     * Copy the attribute changes from the source entity to the target.
     *
     * @param source - Source entity to copy changes from.
     * @param target - Target entity to copy changes to.
     * @param <T>    - Entity Type.
     * @return - Updated Target entity.
     * @throws EntityException
     */
    public static <K extends IKey, T extends IEntity<K>> T copyChanges(@Nonnull T source,
                                                                       @Nonnull T target)
    throws EntityException {
        Preconditions.checkArgument(
                ReflectionUtils.isSuperType(target.getClass(), source.getClass()));
        try {
            Field[] fields = ReflectionUtils.getAllFields(source.getClass());
            if (fields != null && fields.length > 0) {
                for (Field field : fields) {
                    if (field.getName().compareTo(FIELD_ID) == 0) {
                        Object sid = ReflectionUtils.getFieldValue(source, field);
                        Object tid = ReflectionUtils.getFieldValue(target, field);
                        if (tid == null) {
                            ReflectionUtils.setObjectValue(target, field, sid);
                        } else {
                            continue;
                        }
                    }
                    copyField(field, source, target);
                }
            }
            return target;
        } catch (Exception e) {
            throw new EntityException(e);
        }
    }

    /**
     * Copy the field value from the source to the target.
     *
     * @param field  - Field value to copy.
     * @param source - Source entity.
     * @param target - Target entity.
     * @param <T>    - Copiable Type
     * @throws EntityException
     */
    @SuppressWarnings("rawtypes")
    private static <T extends IEntity> void copyField(Field field,
                                                        @Nonnull T source,
                                                        @Nonnull T target)
    throws EntityException {
        try {
            Object value = ReflectionUtils.getFieldValue(source, field);
            if (value == null) {
                ReflectionUtils.setObjectValue(target, field, null);
            } else {
                Class<?> ftype = field.getType();
                if (ReflectionUtils.implementsInterface(IEntity.class, ftype)) {
                    Object tvalue = ReflectionUtils.getFieldValue(target, field);
                    if (tvalue == null) {
                        tvalue = ftype.newInstance();
                        ReflectionUtils.setObjectValue(target, field, tvalue);
                    }
                    copyChanges((IEntity) value, (IEntity) tvalue);
                } else {
                    ReflectionUtils.setObjectValue(target, field, value);
                }
            }
        } catch (Exception e) {
            throw new EntityException(e);
        }
    }
}
