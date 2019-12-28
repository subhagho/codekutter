/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright (c) $year
 * Date: 3/2/19 12:10 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Utility functions to help with Getting/Setting Object/Field values using Reflection.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * <p>
 * 11:10:30 AM
 */
public class ReflectionUtils {
    /**
     * Find the field with the specified name in this type or a parent type.
     *
     * @param type - Class to find the field in.
     * @param name - Field name.
     * @return - Found Field or NULL
     */
    public static Field findField(@Nonnull Class<?> type,
                                  @Nonnull String name) {
        Preconditions.checkArgument(type != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        Field[] fields = type.getDeclaredFields();
        if (fields != null && fields.length > 0) {
            for (Field field : fields) {
                if (field.getName().compareTo(name) == 0) {
                    return field;
                }
            }
        }
        Class<?> parent = type.getSuperclass();
        if (parent != null && !parent.equals(Object.class)) {
            return findField(parent, name);
        }
        return null;
    }

    /**
     * Recursively get all the public methods declared for a type.
     *
     * @param type - Type to fetch fields for.
     * @return - Array of all defined methods.
     */
    public static Method[] getAllMethods(@Nonnull Class<?> type) {
        Preconditions.checkArgument(type != null);
        List<Method> methods = new ArrayList<>();
        getMethods(type, methods);
        if (!methods.isEmpty()) {
            Method[] ma = new Method[methods.size()];
            for (int ii = 0; ii < methods.size(); ii++) {
                ma[ii] = methods.get(ii);
            }
            return ma;
        }
        return null;
    }

    /**
     * Get all public methods declared for this type and add them to the list passed.
     *
     * @param type    - Type to get methods for.
     * @param methods - List of methods.
     */
    private static void getMethods(Class<?> type, List<Method> methods) {
        Method[] ms = type.getDeclaredMethods();
        if (ms != null && ms.length > 0) {
            for (Method m : ms) {
                if (m != null && Modifier.isPublic(m.getModifiers()))
                    methods.add(m);
            }
        }
        Class<?> st = type.getSuperclass();
        if (st != null && !st.equals(Object.class)) {
            getMethods(st, methods);
        }
    }

    /**
     * Recursively get all the declared fields for a type.
     *
     * @param type - Type to fetch fields for.
     * @return - Array of all defined fields.
     */
    public static Field[] getAllFields(@Nonnull Class<?> type) {
        Preconditions.checkArgument(type != null);
        List<Field> fields = new ArrayList<>();
        getFields(type, fields);
        if (!fields.isEmpty()) {
            Field[] fa = new Field[fields.size()];
            for (int ii = 0; ii < fields.size(); ii++) {
                fa[ii] = fields.get(ii);
            }
            return fa;
        }
        return null;
    }

    /**
     * Get fields declared for this type and add them to the list passed.
     *
     * @param type   - Type to get fields for.
     * @param fields - List of fields.
     */
    private static void getFields(Class<?> type, List<Field> fields) {
        Field[] fs = type.getDeclaredFields();
        if (fs != null && fs.length > 0) {
            for (Field f : fs) {
                if (f != null)
                    fields.add(f);
            }
        }
        Class<?> st = type.getSuperclass();
        if (st != null && !st.equals(Object.class)) {
            getFields(st, fields);
        }
    }

    /**
     * Get the String value of the field in the object passed.
     *
     * @param o     - Object to extract field value from.
     * @param field - Field to extract.
     * @return - String value.
     * @throws Exception
     */
    public static String strinfigy(@Nonnull Object o, @Nonnull Field field)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(field != null);

        Object v = getFieldValue(o, field);
        if (v != null) {
            return String.valueOf(v);
        }
        return null;
    }

    /**
     * Get the value of the specified field from the object passed.
     * This assumes standard bean Getters/Setters.
     *
     * @param o     - Object to get field value from.
     * @param field - Field value to extract.
     * @return - Field value.
     * @throws Exception
     */
    public static Object getFieldValue(@Nonnull Object o, @Nonnull Field field)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(field != null);

        String method = "get" + StringUtils.capitalize(field.getName());

        Method m = MethodUtils.getAccessibleMethod(o.getClass(), method);
        if (m == null) {
            method = field.getName();
            m = MethodUtils.getAccessibleMethod(o.getClass(), method);
        }

        if (m == null) {
            Class<?> type = field.getType();
            if (type.equals(boolean.class) || type.equals(Boolean.class)) {
                method = "is" + StringUtils.capitalize(field.getName());
                m = MethodUtils.getAccessibleMethod(o.getClass(), method);
            }
        }

        if (m == null)
            throw new Exception("No accessable method found for field. [field="
                                        + field.getName() + "][class="
                                        + o.getClass().getCanonicalName() + "]");
        return MethodUtils.invokeMethod(o, method);
    }

    /**
     * Check is the field value can be converted to a String value.
     *
     * @param field - Field to check type for.
     * @return - Can convert to String?
     */
    public static boolean canStringify(@Nonnull Field field) {
        Preconditions.checkArgument(field != null);
        if (field.isEnumConstant() || field.getType().isEnum())
            return true;
        if (isPrimitiveTypeOrClass(field))
            return true;
        if (field.getType().equals(String.class))
            return true;
        if (field.getType().equals(Date.class))
            return true;
        return false;
    }

    /**
     * Set the value of a primitive attribute for the specified object.
     *
     * @param value  - Value to set.
     * @param source - Object to set the attribute value.
     * @param f      - Field to set value for.
     * @throws Exception
     */
    public static final void setPrimitiveValue(@Nonnull String value,
                                               @Nonnull Object source,
                                               @Nonnull Field f) throws Exception {
        Preconditions.checkArgument(source != null);
        Preconditions.checkArgument(f != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        Class<?> type = f.getType();
        if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            setBooleanValue(source, f, value);
        } else if (type.equals(short.class) || type.equals(Short.class)) {
            setShortValue(source, f, value);
        } else if (type.equals(int.class) || type.equals(Integer.class)) {
            setIntValue(source, f, value);
        } else if (type.equals(float.class) || type.equals(Float.class)) {
            setFloatValue(source, f, value);
        } else if (type.equals(double.class) || type.equals(Double.class)) {
            setDoubleValue(source, f, value);
        } else if (type.equals(long.class) || type.equals(Long.class)) {
            setLongValue(source, f, value);
        } else if (type.equals(char.class) || type.equals(Character.class)) {
            setCharValue(source, f, value);
        }
    }

    /**
     * Set the value of the field by converting the specified String value to the
     * required value type.
     *
     * @param value  - String value to set.
     * @param source - Object to set the attribute value.
     * @param f      - Field to set value for.
     * @return - Updated object instance.
     * @throws ReflectionException
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static final Object setValueFromString(@Nonnull String value,
                                                  @Nonnull Object source,
                                                  @Nonnull Field f) throws
                                                                    ReflectionException {
        Preconditions.checkArgument(source != null);
        Preconditions.checkArgument(f != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        try {
            Object retV = value;
            Class<?> type = f.getType();
            if (ReflectionUtils
                    .isPrimitiveTypeOrClass(f)) {
                 ReflectionUtils
                        .setPrimitiveValue(value, source, f);
            } else if (type.equals(String.class)) {
                 ReflectionUtils
                        .setStringValue(source, f, value);
            } else if (type.isEnum()) {
                Class<Enum> et = (Class<Enum>) type;
                Object ev = Enum.valueOf(et, value);
                 ReflectionUtils
                        .setObjectValue(source, f, ev);
                retV = ev;
            } else if (type.equals(File.class)) {
                File file = new File(value);
                 ReflectionUtils
                        .setObjectValue(source, f, file);
                retV = file;
            } else if (type.equals(Class.class)) {
                Class<?> cls = Class.forName(value.trim());
                 ReflectionUtils
                        .setObjectValue(source, f, cls);
                retV = cls;
            } else {
                Class<?> cls = Class.forName(value.trim());
                if (type.isAssignableFrom(cls)) {
                    Object o = cls.newInstance();
                     ReflectionUtils
                            .setObjectValue(source, f, o);
                    retV = o;
                } else {
                    throw new InstantiationException(
                            "Cannot create instance of type [type="
                                    + cls.getCanonicalName()
                                    + "] and assign to field [field="
                                    + f.getName() + "]");
                }
            }
            return retV;
        } catch (Exception e) {
            throw new ReflectionException(
                    "Error setting object value : [type="
                            + source.getClass().getCanonicalName() + "][field="
                            + f.getName() + "]",
                    e);
        }
    }

    /**
     * Set the value of the specified field in the object to the value passed.
     *
     * @param o     - Object to set value for.
     * @param f     - Field to set value for.
     * @param value - Value to set to.
     * @throws Exception
     */
    public static void setObjectValue(@Nonnull Object o, @Nonnull Field f,
                                      Object value)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(f != null);

        String method = "set" + StringUtils.capitalize(f.getName());
        Method m = MethodUtils.getAccessibleMethod(o.getClass(), method,
                                                   f.getType());
        if (m == null) {
            method = f.getName();
            m = MethodUtils.getAccessibleMethod(o.getClass(), method,
                                                f.getType());
        }

        if (m == null)
            throw new Exception("No accessable method found for field. [field="
                                        + f.getName() + "][class=" +
                                        o.getClass().getCanonicalName()
                                        + "]");
        MethodUtils.invokeMethod(o, method, value);
    }

    /**
     * Set the value of the field to the passed String value.
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setStringValue(@Nonnull Object o, @Nonnull Field f,
                                      String value)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(f != null);

        setObjectValue(o, f, value);
    }

    /**
     * Set the value of the field to boolean value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setBooleanValue(@Nonnull Object o, @Nonnull Field f,
                                       @Nonnull String value)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(f != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        boolean bv = Boolean.valueOf(value);
        setObjectValue(o, f, bv);
    }

    /**
     * Set the value of the field to Short value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setShortValue(@Nonnull Object o, @Nonnull Field f,
                                     @Nonnull String value)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(f != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        short sv = Short.parseShort(value);
        setObjectValue(o, f, sv);
    }

    /**
     * Set the value of the field to Integer value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setIntValue(@Nonnull Object o, @Nonnull Field f,
                                   @Nonnull String value)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(f != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        int iv = Integer.parseInt(value);
        setObjectValue(o, f, iv);
    }

    /**
     * Set the value of the field to Long value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setLongValue(@Nonnull Object o, @Nonnull Field f,
                                    @Nonnull String value)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(f != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        long lv = Long.parseLong(value);
        setObjectValue(o, f, lv);
    }

    /**
     * Set the value of the field to Float value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setFloatValue(@Nonnull Object o, @Nonnull Field f,
                                     @Nonnull String value)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(f != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        float fv = Float.parseFloat(value);
        setObjectValue(o, f, fv);
    }

    /**
     * Set the value of the field to Double value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setDoubleValue(@Nonnull Object o, @Nonnull Field f,
                                      @Nonnull String value)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(f != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        double dv = Double.parseDouble(value);
        setObjectValue(o, f, dv);
    }

    /**
     * Set the value of the field to Char value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setCharValue(@Nonnull Object o, @Nonnull Field f,
                                    @Nonnull String value)
    throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(f != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        char cv = value.charAt(0);
        setObjectValue(o, f, cv);
    }

    /**
     * Check if the specified field is a primitive or primitive type class.
     *
     * @param field - Field to check primitive for.
     * @return - Is primitive?
     */
    public static boolean isPrimitiveTypeOrClass(@Nonnull Field field) {
        Class<?> type = field.getType();
        return isPrimitiveTypeOrClass(type);
    }

    /**
     * Check if the specified type is a primitive or primitive type class.
     *
     * @param type - Field to check primitive for.
     * @return - Is primitive?
     */
    public static boolean isPrimitiveTypeOrClass(@Nonnull Class<?> type) {
        if (type.isPrimitive())
            return true;
        else if (type.equals(Boolean.class) || type.equals(boolean.class) ||
                type.equals(Short.class) || type.equals(short.class)
                || type.equals(Integer.class) || type.equals(int.class) ||
                type.equals(Long.class) || type.equals(long.class)
                || type.equals(Float.class) || type.equals(float.class) ||
                type.equals(Double.class) || type.equals(double.class)
                || type.equals(Character.class) || type.equals(char.class)) {
            return true;
        }
        return false;
    }

    /**
     * Check if the specified field is a primitive or primitive type class or String.
     *
     * @param field - Field to check primitive/String for.
     * @return - Is primitive or String?
     */
    public static boolean isPrimitiveTypeOrString(@Nonnull Field field) {
        Class<?> type = field.getType();
        return isPrimitiveTypeOrString(type);
    }

    /**
     * Check if the specified type is a primitive or primitive type class or String.
     *
     * @param type - Field to check primitive/String for.
     * @return - Is primitive or String?
     */
    public static boolean isPrimitiveTypeOrString(@Nonnull Class<?> type) {
        if (isPrimitiveTypeOrClass(type)) {
            return true;
        }
        if (type == String.class) {
            return true;
        }
        return false;
    }

    /**
     * Check if the parent type specified is an ancestor (inheritance) of the passed type.
     *
     * @param parent - Ancestor type to check.
     * @param type   - Inherited type
     * @return - Is Ancestor type?
     */
    public static boolean isSuperType(@Nonnull Class<?> parent,
                                      @Nonnull Class<?> type) {
        Preconditions.checkArgument(parent != null);
        Preconditions.checkArgument(type != null);
        if (parent.equals(type)) {
            return true;
        } else if (type.equals(Object.class)) {
            return false;
        } else {
            Class<?> pp = type.getSuperclass();
            if (pp == null) {
                return false;
            }
            return isSuperType(parent, pp);
        }
    }

    /**
     * Check is the passed type (or its ancestor) implements the specified interface.
     *
     * @param intf - Interface type to check.
     * @param type - Type implementing expected interface.
     * @return - Implements Interface?
     */
    public static boolean implementsInterface(@Nonnull Class<?> intf,
                                              @Nonnull Class<?> type) {
        Preconditions.checkArgument(intf != null);
        Preconditions.checkArgument(type != null);

        if (intf.equals(type)) {
            return true;
        }
        Class<?>[] intfs = type.getInterfaces();
        if (intfs != null && intfs.length > 0) {
            for (Class<?> itf : intfs) {
                if (isSuperType(intf, itf)) {
                    return true;
                }
            }
        }
        Class<?> parent = type.getSuperclass();
        if (parent != null && !parent.equals(Object.class)) {
            return implementsInterface(intf, parent);
        }
        return false;
    }

    /**
     * Get the Parameterized type of the List field specified.
     *
     * @param field - Field to extract the Parameterized type for.
     * @return - Parameterized type.
     * @throws Exception
     */
    public static Class<?> getGenericListType(@Nonnull Field field)
    throws Exception {
        Preconditions.checkArgument(field != null);
        Preconditions
                .checkArgument(implementsInterface(List.class, field.getType()));

        ParameterizedType ptype = (ParameterizedType) field.getGenericType();
        return (Class<?>) ptype.getActualTypeArguments()[0];
    }

    /**
     * Get the Parameterized type of the Set field specified.
     *
     * @param field - Field to extract the Parameterized type for.
     * @return - Parameterized type.
     * @throws Exception
     */
    public static Class<?> getGenericSetType(@Nonnull Field field)
    throws Exception {
        Preconditions.checkArgument(field != null);
        Preconditions
                .checkArgument(implementsInterface(Set.class, field.getType()));

        ParameterizedType ptype = (ParameterizedType) field.getGenericType();
        return (Class<?>) ptype.getActualTypeArguments()[0];
    }

    /**
     * Get the parsed value of the type specified from the
     * string value passed.
     *
     * @param type - Required value type
     * @param value - Input String value
     * @return - Parsed Value.
     */
    @SuppressWarnings("unchecked")
    public static Object parseStringValue(Class<?> type, String value) {
        if (!Strings.isNullOrEmpty(value)) {
            if (isPrimitiveTypeOrString(type)) {
                return parsePrimitiveValue(type, value);
            } else if (type.isEnum()) {
                Class<Enum> et = (Class<Enum>) type;
                return Enum.valueOf(et, value);
            }
        }
        return null;
    }

    /**
     * Get the value of the primitive type parsed from the string value.
     *
     * @param type - Primitive Type
     * @param value - String value
     * @return - Parsed Value
     */
    private static Object parsePrimitiveValue(Class<?> type, String value) {
        if (type.equals(Boolean.class) || type.equals(boolean.class)) {
            return Boolean.parseBoolean(value);
        } else if (type.equals(Short.class) || type.equals(short.class)) {
            return Short.parseShort(value);
        } else if (type.equals(Integer.class) || type.equals(int.class)) {
            return Integer.parseInt(value);
        } else if (type.equals(Long.class) || type.equals(long.class)) {
            return Long.parseLong(value);
        } else if (type.equals(Float.class) || type.equals(float.class)) {
            return Float.parseFloat(value);
        } else if (type.equals(Double.class) || type.equals(double.class)) {
            return Double.parseDouble(value);
        } else if (type.equals(Character.class) || type.equals(char.class)) {
            return value.charAt(0);
        } else if (type.equals(Byte.class) || type.equals(byte.class)) {
            return Byte.parseByte(value);
        } else if (type.equals(String.class)) {
            return value;
        }
        return null;
    }
}