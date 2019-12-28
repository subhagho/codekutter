package com.codekutter.common.model;

import java.util.Collection;

/**
 * Check for NULL/Empty values.
 *
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
