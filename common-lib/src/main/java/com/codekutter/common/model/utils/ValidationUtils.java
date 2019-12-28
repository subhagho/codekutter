package com.codekutter.common.model.utils;

import com.codekutter.common.model.*;
import com.codekutter.common.utils.ReflectionUtils;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to automate field/property validations.
 */
public class ValidationUtils {
    private static final Map<String, IValidationConstraint> constraints = new HashMap<>();

    /**
     * Validate the passed object instance.
     *
     * @param type - Type
     * @param value - Value
     * @param <T> - Object Type to validate.
     * @throws ValidationExceptions
     */
    public static <T> void validate(@Nonnull Class<? extends T> type, T value) throws ValidationExceptions {
        Preconditions.checkArgument(type != null);
        ValidationExceptions errors = null;
        if (value == null) {
            errors = addError(type.getCanonicalName(), "Value is NULL.", errors);
            throw errors;
        }
        try {
            Field[] fields = ReflectionUtils.getAllFields(type);
            if (fields != null && fields.length > 0) {
                for (Field field : fields) {
                    Object v = ReflectionUtils.getFieldValue(value, field);
                    if (field.isAnnotationPresent(Validate.class)) {
                        Validate validate = field.getAnnotation(Validate.class);
                        IValidationConstraint constraint = getConstraint(validate.constraint());
                       try {
                            constraint.validate(field.getName(), type, v);
                        } catch (ValidationException ve) {
                            addError(ve, errors);
                        }
                    }
                    if (v instanceof IValidate) {
                        try {
                            ((IValidate) v).validate();
                        } catch (ValidationExceptions ves) {
                            if (!ves.getErrors().isEmpty()) {
                                if (errors == null) {
                                    errors = new ValidationExceptions();
                                }
                                errors.addAll(ves.getErrors());
                            }
                        }
                    }
                }
            }
            if (errors != null) {
                throw errors;
            }
        } catch (Exception ex) {
            errors = addError(type.getCanonicalName(), ex.getLocalizedMessage(), errors);
            throw errors;
        }
    }

    /**
     * Get/Create a validator instance based on the classname.
     *
     * @param classanme - Validator Class
     * @return - Validator Instance.
     * @throws Exception
     */
    private static IValidationConstraint getConstraint(String classanme) throws Exception {
        if (!constraints.containsKey(classanme)) {
            Class<?> cls = Class.forName(classanme);
            Object obj = cls.newInstance();
            if (obj instanceof IValidationConstraint) {
                constraints.put(classanme, (IValidationConstraint) obj);
            } else {
                throw new Exception(String.format("Invalid Validator specified: [type=%s]", cls.getCanonicalName()));
            }
        }
        return constraints.get(classanme);
    }

    private static ValidationExceptions addError(String property, String message, ValidationExceptions error) {
        if (error == null) {
            error = new ValidationExceptions();
        }
        error.add(new ValidationException(String.format("Validation failed : [property=%s][error=%s]", property, message)));

        return error;
    }

    private static ValidationExceptions addError(ValidationException ve, ValidationExceptions error) {
        if (error == null) {
            error = new ValidationExceptions();
        }
        error.add(ve);

        return error;
    }

}
