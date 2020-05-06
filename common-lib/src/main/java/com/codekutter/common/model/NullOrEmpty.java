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

package com.codekutter.common.model;

import java.util.Collection;

/**
 * Check for NULL/Empty values.
 * <p>
 * Empty values will be checked for String/Collections.
 */
public class NullOrEmpty implements IValidationConstraint {
    public static final String DEFAULT = NullOrEmpty.class.getCanonicalName();

    /**
     * Validate the input value.
     *
     * @param property - Property being validated.
     * @param type     - Type of object being validated.
     * @param value    - Input value.
     * @throws ValidationException - Exception will be thrown on validation failure.
     */
    @Override
    public void validate(String property, Class<?> type, Object value) throws ValidationException {
        ValidationException.checkNotNull(type.getCanonicalName(), value);
        if (value instanceof String) {
            ValidationException.checkNotEmpty(property, (String) value);
        }
        if (value instanceof Collection) {
            if (((Collection) value).isEmpty()) {
                throw new ValidationException(String.format("Empty collection: {property=%s[%s]}", type.getCanonicalName(), property));
            }
        }
    }
}
