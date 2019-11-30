package com.codekutter.common.model;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Embeddable;

/**
 * Struct to define update information.
 */
@Embeddable
@Getter
@Setter
public class UpdateInfo implements IValidate {
    /**
     * Updated By User ID
     */
    private String updatedBy;
    /**
     * Updated At timestamp.
     */
    private long updateTimestamp = -1;

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        ValidationExceptions errors = null;
        if (Strings.isNullOrEmpty(updatedBy)) {
            errors = ValidationExceptions
                    .add(new ValidationException("Updated By User not specified"),
                         errors);
        }
        if (updateTimestamp <= 0) {
            errors = ValidationExceptions
                    .add(new ValidationException("Updated Timestamp not specified"),
                         errors);
        }
        if (errors != null) {
            throw errors;
        }
    }
}
