package com.codekutter.common.model;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

/**
 * Exception type to be used for escalating Validation errors.
 */
public class ValidationException extends Exception {
    private static final String __PREFIX__ = "Validation Error : %s";

    /**
     * Constructor with Error message.
     *
     * @param mesg - Error Message.
     */
    public ValidationException(String mesg) {
        super(String.format(__PREFIX__, mesg));
    }

    /**
     * Constructor with Error message and root cause.
     *
     * @param mesg  - Error Message.
     * @param cause - Cause.
     */
    public ValidationException(String mesg, Throwable cause) {
        super(String.format(__PREFIX__, mesg), cause);
    }

    /**
     * Constructor with root cause.
     *
     * @param cause - Cause.
     */
    public ValidationException(Throwable cause) {
        super(String.format(__PREFIX__, cause.getLocalizedMessage()), cause);
    }

    /**
     * Validate that a required property value is not NULL.
     *
     * @param property - Property name.
     * @param value    - Property value.
     * @throws ValidationException
     */
    public static void checkNotNull(@Nonnull String property, Object value)
    throws ValidationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));
        if (value == null) {
            throw new ValidationException(
                    String.format("Property value is NULL. [property=%s]",
                                  property));
        }
    }

    /**
     * Validate that a required property value is not NULL.
     *
     * @param property - Property name.
     * @param value    - Property value.
     * @throws ValidationException
     */
    public static ValidationExceptions checkNotNull(@Nonnull String property, Object value,
                                    ValidationExceptions errors) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));
        if (value == null) {
            ValidationException ve = new ValidationException(
                    String.format("Property value is NULL. [property=%s]",
                                  property));
            errors = ValidationExceptions.add(ve, errors);
        }
        return errors;
    }

    /**
     * Validate a required property value is not NULL/Empty.
     *
     * @param property - Property name.
     * @param value    - Property value.
     * @throws ValidationException
     */
    public static void checkNotEmpty(@Nonnull String property, String value)
    throws ValidationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));
        if (Strings.isNullOrEmpty(value)) {
            throw new ValidationException(
                    String.format("Property value is NULL/Empty. [property=%s]",
                                  property));
        }
    }

    /**
     * Validate a required property value is not NULL/Empty.
     *
     * @param property - Property name.
     * @param value    - Property value.
     * @param errors   - Handle for error wrapper.
     * @return - Handle for error wrapper.
     * @throws ValidationException
     */
    public static ValidationExceptions checkNotEmpty(@Nonnull String property,
                                                     String value,
                                                     ValidationExceptions errors) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));
        if (Strings.isNullOrEmpty(value)) {
            ValidationException ve = new ValidationException(
                    String.format("Property value is NULL/Empty. [property=%s]",
                                  property));
            errors = ValidationExceptions.add(ve, errors);
        }
        return errors;
    }
}
