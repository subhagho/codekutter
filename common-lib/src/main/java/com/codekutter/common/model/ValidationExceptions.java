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

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.NotImplementedException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Exception type for escalating validation error(s).
 */
@Getter
@Setter
public class ValidationExceptions extends Exception
        implements Collection<ValidationException> {
    private static final String __MESG__ = "Validation Error detected.";

    /**
     * List of validation errors.
     */
    private List<ValidationException> errors;

    /**
     * Default empty constructor.
     */
    public ValidationExceptions() {
        super(__MESG__);
    }

    /**
     * Constructor with list of validation errors.
     *
     * @param errors - Validation Errors.
     */
    public ValidationExceptions(List<ValidationException> errors) {
        super(__MESG__);
        this.errors = errors;
    }

    /**
     * Add a validation error.
     *
     * @param error - Validation Error
     * @return - Self.
     */
    @Override
    public boolean add(@Nonnull ValidationException error) {
        Preconditions.checkArgument(error != null);
        if (errors == null) {
            errors = new ArrayList<>();
        }
        return errors.add(error);
    }

    /**
     * Add the list of Validation errors.
     *
     * @param errs - Validation Errors.
     * @return - Self.
     */
    public ValidationExceptions addAll(@Nonnull List<ValidationException> errs) {
        Preconditions.checkArgument(errs != null);
        Preconditions.checkArgument(!errs.isEmpty());
        if (errors == null) {
            errors = new ArrayList<>();
        }
        errors.addAll(errs);

        return this;
    }

    @Override
    public int size() {
        return (errors != null ? errors.size() : 0);
    }

    @Override
    public boolean isEmpty() {
        return (errors != null ? errors.isEmpty() : true);
    }

    @Override
    public boolean contains(Object o) {
        if (errors != null && (o instanceof ValidationException)) {
            return errors.contains(o);
        }
        return false;
    }

    @Override
    public @Nonnull Iterator<ValidationException> iterator() {
        Preconditions.checkState(errors != null);
        return errors.iterator();
    }

    @Override
    public @Nonnull Object[] toArray() {
        if (errors != null) {
            return errors.toArray();
        }
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        throw new NotImplementedException("Typed toArray not implemented.");
    }

    @Override
    public boolean remove(Object o) {
        if (errors != null) {
            return errors.remove(o);
        }
        return false;
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> collection) {
        if (errors != null) {
            return errors.containsAll(collection);
        }
        return false;
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends ValidationException> collection) {
        if (errors == null) {
            errors = new ArrayList<>();
        }

        return errors.addAll(collection);
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> collection) {
        if (errors != null) {
            return errors.removeAll(collection);
        }
        return false;
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> collection) {
        if (errors != null) {
            return errors.retainAll(collection);
        }
        return false;
    }

    @Override
    public void clear() {
        if (errors != null) {
            errors.clear();
        }
    }

    /**
     * Add a error instance to the Errors handle. Will check and create a errors
     * instance if required.
     *
     * @param error  - Error handle.
     * @param errors - Errors collection instance.
     * @return - Errors Collection.
     */
    public static ValidationExceptions add(@Nonnull ValidationException error,
                                           ValidationExceptions errors) {
        Preconditions.checkArgument(error != null);
        if (errors == null) {
            errors = new ValidationExceptions();
        }
        errors.add(error);
        return errors;
    }

    /**
     * Copy all the validation errors from the source to the target.
     *
     * @param source - Source Error Collection.
     * @param target - Target Error Collection.
     * @return - Error Collection.
     */
    public static ValidationExceptions copy(@Nonnull ValidationExceptions source,
                                            ValidationExceptions target) {
        if (target == null) {
            return source;
        }
        if (source.errors != null && !source.errors.isEmpty()) {
            target = target.addAll(source.errors);
        }
        return target;
    }
}
