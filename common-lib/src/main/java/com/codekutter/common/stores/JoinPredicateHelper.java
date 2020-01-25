package com.codekutter.common.stores;

import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.annotations.Reference;
import com.codekutter.common.utils.KeyValuePair;
import com.codekutter.common.utils.ReflectionUtils;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class JoinPredicateHelper {
    public static <T extends IEntity> String generateHibernateJoinQuery(@Nonnull Reference reference,
                                                                        @Nonnull Collection<T> source,
                                                                        @Nonnull Field field,
                                                                        @Nonnull DataStoreManager manager,
                                                                        boolean appendQuery) throws DataStoreException {
        try {
            Class<?> type = field.getType();
            if (ReflectionUtils.implementsInterface(List.class, type)) {
                type = ReflectionUtils.getGenericListType(field);
            } else if (ReflectionUtils.implementsInterface(Set.class, type)) {
                type = ReflectionUtils.getGenericSetType(field);
            }
            if (!type.equals(reference.target())) {
                throw new DataStoreException(String.format("Specified field type invalid. [type=%s][reference=%s]",
                        type.getCanonicalName(), reference.target().getCanonicalName()));
            }
            if (!manager.isTypeSupported(reference.target())) {
                throw new DataStoreException(String.format("Specified entity type not supported. [type=%s]", type.getCanonicalName()));
            }
            StringBuffer buffer = new StringBuffer();
            boolean first = true;
            for (IEntity entity : source) {
                String condition = getHibernateJoinConditions(reference.columns(), entity, reference.target());
                if (Strings.isNullOrEmpty(condition)) {
                    throw new DataStoreException(String.format("Error generating condition. [type=%s][key=%s]",
                            entity.getClass().getCanonicalName(), entity.getKey()));
                }
                if (appendQuery && !Strings.isNullOrEmpty(reference.query())) {
                    condition = String.format("%s AND (%s)", condition, reference.query());
                }
                if (first) first = false;
                else buffer.append(" OR ");
                buffer.append("(").append(condition).append(")");
            }

            return buffer.toString();
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public static <T extends IEntity> String generateHibernateJoinQuery(@Nonnull Reference reference,
                                                                        @Nonnull T source,
                                                                        @Nonnull Field field,
                                                                        @Nonnull DataStoreManager manager,
                                                                        boolean appendQuery) throws DataStoreException {
        try {
            Class<?> type = field.getType();
            if (ReflectionUtils.implementsInterface(List.class, type)) {
                type = ReflectionUtils.getGenericListType(field);
            } else if (ReflectionUtils.implementsInterface(Set.class, type)) {
                type = ReflectionUtils.getGenericSetType(field);
            }
            if (!type.equals(reference.target())) {
                throw new DataStoreException(String.format("Specified field type invalid. [type=%s][reference=%s]",
                        type.getCanonicalName(), reference.target().getCanonicalName()));
            }
            if (!manager.isTypeSupported(reference.target())) {
                throw new DataStoreException(String.format("Specified entity type not supported. [type=%s]", type.getCanonicalName()));
            }
            String condition = getHibernateJoinConditions(reference.columns(), source, reference.target());
            if (Strings.isNullOrEmpty(condition)) {
                throw new DataStoreException(String.format("Error generating JOIN condition. [type=%s][field=%s]",
                        source.getClass().getCanonicalName(), field.getName()));
            }
            if (appendQuery && !Strings.isNullOrEmpty(reference.query())) {
                condition = String.format("%s AND (%s)", condition, reference.query());
            }
            return condition;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public static String generateSearchQuery(@Nonnull Reference reference,
                                             @Nonnull Collection<IEntity> source,
                                             @Nonnull Field field,
                                             @Nonnull DataStoreManager manager) throws DataStoreException {

        return null;
    }

    public static String generateSearchQuery(@Nonnull Reference reference,
                                             @Nonnull IEntity source,
                                             @Nonnull Field field,
                                             @Nonnull DataStoreManager manager) throws DataStoreException {

        return null;
    }

    private static String getHibernateJoinConditions(JoinColumns joinColumns,
                                                     IEntity source,
                                                     Class<? extends IEntity> type) throws DataStoreException {
        JoinColumn[] columns = joinColumns.value();
        if (columns.length > 0) {
            StringBuffer buffer = new StringBuffer();
            for (JoinColumn column : columns) {
                String vc = getHibernateJoinCondition(column, source, type);
                if (Strings.isNullOrEmpty(vc)) {
                    throw new DataStoreException(String.format("Error generating JOIN condition. [type=%s][column=%s]",
                            type.getCanonicalName(), column.name()));
                }
                if (buffer.length() > 0) {
                    buffer.append(" AND ");
                }
                buffer.append(vc);
            }
            return buffer.toString();
        }
        return null;
    }

    private static String getHibernateJoinCondition(JoinColumn column,
                                                    IEntity source,
                                                    Class<? extends IEntity> type) throws DataStoreException {
        try {
            KeyValuePair<String, Field> kv = getHibernateFieldName(column, type);
            if (kv != null) {
                String cname = column.name();
                Object value = ReflectionUtils.getNestedFieldValue(source, cname);
                if (value != null) {
                    String vc = getHibernateValueCondition(kv.value(), value);
                    if (!Strings.isNullOrEmpty(vc)) {
                        return String.format("(%s %s)", kv.key(), vc);
                    }
                } else {
                    return String.format("(%s == null)", kv.key());
                }
            }
            return null;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    private static String getHibernateValueCondition(Field field, Object value) throws Exception {
        if (ReflectionUtils.isPrimitiveTypeOrString(field)) {
            if (field.getType().isEnum() || field.getType().equals(String.class)) {
                return String.format(" = '%s'", String.valueOf(value));
            } else {
                return String.format(" = %s", String.valueOf(value));
            }
        } else if (ReflectionUtils.implementsInterface(List.class, field.getType())) {
            Class<?> type = ReflectionUtils.getGenericListType(field);
            if (ReflectionUtils.isPrimitiveTypeOrString(type)) {
                StringBuffer buffer = new StringBuffer("IN (");
                List<?> list = (List<?>) value;
                boolean first = true;
                if (type.isEnum() || type.equals(String.class)) {
                    for (Object v : list) {
                        if (first) first = false;
                        else buffer.append(", ");
                        buffer.append(String.format(" = '%s'", String.valueOf(v)));
                    }
                } else {
                    for (Object v : list) {
                        if (first) first = false;
                        else buffer.append(", ");
                        buffer.append(String.format(" = %s", String.valueOf(v)));
                    }
                }
                buffer.append(")");
                return buffer.toString();
            } else {
                throw new Exception(String.format("Cannot set condition for type. [type=%s]", type.getCanonicalName()));
            }
        } else if (ReflectionUtils.implementsInterface(Set.class, field.getType())) {
            Class<?> type = ReflectionUtils.getGenericListType(field);
            if (ReflectionUtils.isPrimitiveTypeOrString(type)) {
                StringBuffer buffer = new StringBuffer("IN (");
                Set<?> list = (Set<?>) value;
                boolean first = true;
                if (type.isEnum() || type.equals(String.class)) {
                    for (Object v : list) {
                        if (first) first = false;
                        else buffer.append(", ");
                        buffer.append(String.format(" '%s'", String.valueOf(v)));
                    }
                } else {
                    for (Object v : list) {
                        if (first) first = false;
                        else buffer.append(", ");
                        buffer.append(String.format(" %s", String.valueOf(v)));
                    }
                }
                buffer.append(")");
                return buffer.toString();
            } else {
                throw new Exception(String.format("Cannot set condition for type. [type=%s]", type.getCanonicalName()));
            }
        }
        return null;
    }

    private static KeyValuePair<String, Field> getHibernateFieldName(JoinColumn column, Class<? extends IEntity> type) {
        String cname = column.referencedColumnName();
        if (!Strings.isNullOrEmpty(cname)) {
            return ReflectionUtils.findNestedField(type, cname);
        }
        return null;
    }
}
