package com.codekutter.common.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.MappedSuperclass;

/**
 * Base class for defining Stateful Entities.
 *
 * @param <K> - Entity Key Type.
 */
@Getter
@Setter
@MappedSuperclass
public abstract class StatefulEntity<K> extends RecordVersionedEntity<K> {
    @Column(name = "userState")
    @Enumerated(EnumType.STRING)
    private EntityState state = new EntityState();

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        ValidationExceptions errors = null;
        if (getState().getState() == EEntityState.Unknown) {
            errors = ValidationExceptions.add(new ValidationException(
                    String.format("Entity State is Unknown : [id=%s]",
                                  getKey().toString())), errors);
        }
        errors = validate(errors);

        if (errors != null) {
            getState().setError(errors);
            throw errors;
        }
    }

    /**
     * Validate the derived entity.
     *
     * @param errors - Errors handle.
     */
    public abstract ValidationExceptions validate(ValidationExceptions errors);
}
