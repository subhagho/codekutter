package com.codekutter.common.model;

/**
 * Interface to be implemented for defining auto-validation constraints.
 */
public interface IValidationConstraint {
    /**
     * Validate the input value.
     *
     * @param property - Property being validated.
     * @param type - Type of object being validated.
     * @param value - Input value.
     * @throws ValidationException - Exception will be thrown on validation failure.
     */
    void validate(String property, Class<?> type, Object value) throws ValidationException;
}
