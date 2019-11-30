package com.codekutter.common.model.utils;

import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.annotations.CopyType;
import com.codekutter.common.model.annotations.ECopyType;
import com.codekutter.common.model.CopyException;
import com.codekutter.zconfig.common.Context;
import com.codekutter.zconfig.common.utils.ReflectionUtils;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.persistence.EmbeddedId;
import javax.persistence.Id;
import java.lang.reflect.Field;

public class CopyUtils {
    public static IEntity<?> copy(@Nonnull IEntity<?> target,
                                  @Nonnull IEntity<?> source, Context context)
    throws
    CopyException {
        Preconditions.checkArgument(source != null);
        Preconditions.checkArgument(target != null);
        Preconditions.checkArgument(
                ReflectionUtils.isSuperType(target.getClass(), source.getClass()));
        Field[] fields = ReflectionUtils.getAllFields(source.getClass());
        if (fields != null && fields.length > 0) {
            for (Field field : fields) {
                copyField(target, source, field, context);
            }
        }
        return target;
    }

    private static void copyField(IEntity<?> target, IEntity<?> source,
                                  Field field, Context context)
    throws CopyException {
        try {
            CopyType ct = null;
            Class<?> type = field.getType();
            if (type.isAnnotationPresent(CopyType.class)) {
                ct = type.getAnnotation(CopyType.class);
                if (ct.type() == ECopyType.Ignore) {
                    return;
                }
            }
            if (ReflectionUtils.isPrimitiveTypeOrString(field) || type.isEnum()) {
                Object sv = ReflectionUtils.getFieldValue(source, field);
                ReflectionUtils.setObjectValue(target, field, sv);
            } else {
                Object sv = ReflectionUtils.getFieldValue(source, field);
                if (ct != null && ct.type() == ECopyType.Reference) {
                    ReflectionUtils.setObjectValue(target, field, sv);
                } else if (ct != null) {
                    if (type.isAnnotationPresent(Id.class) ||
                            type.isAnnotationPresent(
                                    EmbeddedId.class)) {
                        return;
                    }
                    if (ReflectionUtils.implementsInterface(IEntity.class, type)) {
                        Object tv = ReflectionUtils.getFieldValue(target, field);
                        if (tv == null) {
                            tv = ((IEntity<?>) sv).clone(context);
                        } else
                            tv = copy((IEntity<?>) tv, (IEntity<?>) sv, context);
                        ReflectionUtils.setObjectValue(target, field, tv);
                    } else {
                        ReflectionUtils.setObjectValue(target, field, sv);
                    }
                } else {
                    ReflectionUtils.setObjectValue(target, field, null);
                }
            }
        } catch (Exception ex) {
            throw new CopyException(ex);
        }
    }
}
