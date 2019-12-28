package com.codekutter.common.model;

/**
 * Interface to define a validation method.
 */
public interface IValidate {
    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    void validate() throws ValidationExceptions;
}
