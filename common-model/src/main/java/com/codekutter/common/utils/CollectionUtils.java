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
 * Date: 3/2/19 7:18 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.utils;


import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CollectionUtils {

    /**
     * Set the value of the specified field to a list of elements converted from the
     * List of input strings.
     * <pre>
     * Supported Lists:
     *      Boolean
     *      Char
     *      Short
     *      Integer
     *      Long
     *      Float
     *      Double
     *      BigInteger
     *      BigDecimal
     *      Date (java.util)
     * </pre>
     *
     * @param source - Source object to set the field value in.
     * @param field  - Field in the source type to set.
     * @param values - List of string values to convert from.
     * @throws Exception
     */
    public static final void setListValues(@Nonnull Object source,
                                           @Nonnull Field field,
                                           @Nonnull List<String> values)
            throws Exception {
        Preconditions.checkArgument(source != null);
        Preconditions.checkArgument(field != null);
        Preconditions.checkArgument(values != null);

        Class<?> type = field.getType();
        if (!type.equals(List.class)) {
            throw new Exception(
                    String.format("Invalid field type. [expected=%s][actual=%s]",
                            List.class.getCanonicalName(),
                            type.getCanonicalName()));
        }
        Class<?> ptype = ReflectionUtils.getGenericListType(field);
        Preconditions.checkNotNull(ptype);
        if (ptype.equals(String.class)) {
            ReflectionUtils.setObjectValue(source, field, values);
        } else if (ptype.equals(Boolean.class)) {
            List<Boolean> bl = createBoolList(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Character.class)) {
            List<Character> bl = createCharList(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Short.class)) {
            List<Short> bl = createShortList(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Integer.class)) {
            List<Integer> bl = createIntList(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Long.class)) {
            List<Long> bl = createLongList(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Float.class)) {
            List<Float> bl = createFloatList(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Double.class)) {
            List<Double> bl = createDoubleList(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(BigInteger.class)) {
            List<BigInteger> bl = createBigIntegerList(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(BigDecimal.class)) {
            List<BigDecimal> bl = createBigDecimalList(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Date.class)) {
            List<Date> bl = createDateList(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        }
    }

    private static List<Boolean> createBoolList(List<String> values) {
        List<Boolean> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Boolean.parseBoolean(value));
        }
        return nvalues;
    }

    private static List<Character> createCharList(List<String> values) {
        List<Character> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(value.charAt(0));
        }
        return nvalues;
    }

    private static List<Short> createShortList(List<String> values) {
        List<Short> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Short.parseShort(value));
        }
        return nvalues;
    }

    private static List<Integer> createIntList(List<String> values) {
        List<Integer> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Integer.parseInt(value));
        }
        return nvalues;
    }

    private static List<Long> createLongList(List<String> values) {
        List<Long> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Long.parseLong(value));
        }
        return nvalues;
    }

    private static List<Float> createFloatList(List<String> values) {
        List<Float> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Float.parseFloat(value));
        }
        return nvalues;
    }

    private static List<Double> createDoubleList(List<String> values) {
        List<Double> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Double.parseDouble(value));
        }
        return nvalues;
    }

    private static List<BigInteger> createBigIntegerList(List<String> values) {
        List<BigInteger> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(new BigInteger(value));
        }
        return nvalues;
    }

    private static List<BigDecimal> createBigDecimalList(List<String> values) {
        List<BigDecimal> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(new BigDecimal(value));
        }
        return nvalues;
    }

    private static List<Date> createDateList(List<String> values) {
        List<Date> nvalues = new ArrayList<>(values.size());
        SimpleDateFormat format = new SimpleDateFormat();
        try {
            for (String value : values) {
                Date dt = format.parse(value);
                nvalues.add(dt);
            }
            return nvalues;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set the value of the specified field to a Set of elements converted from the
     * List of input strings.
     * <pre>
     * Supported Lists:
     *      Boolean
     *      Char
     *      Short
     *      Integer
     *      Long
     *      Float
     *      Double
     *      BigInteger
     *      BigDecimal
     *      Date (java.util)
     * </pre>
     *
     * @param source - Source object to set the field value in.
     * @param field  - Field in the source type to set.
     * @param values - List of string values to convert from.
     * @throws Exception
     */
    public static final void setSetValues(@Nonnull Object source,
                                          @Nonnull Field field,
                                          @Nonnull List<String> values)
            throws Exception {
        Preconditions.checkArgument(source != null);
        Preconditions.checkArgument(field != null);
        Preconditions.checkArgument(values != null);

        Class<?> type = field.getType();
        if (!type.equals(Set.class)) {
            throw new Exception(
                    String.format("Invalid field type. [expected=%s][actual=%s]",
                            Set.class.getCanonicalName(),
                            type.getCanonicalName()));
        }
        Class<?> ptype = ReflectionUtils.getGenericSetType(field);
        Preconditions.checkNotNull(ptype);
        if (ptype.equals(String.class)) {
            Set<String> nvalues = new HashSet<>(values.size());
            nvalues.addAll(values);
            ReflectionUtils.setObjectValue(source, field, nvalues);
        } else if (ptype.equals(Boolean.class)) {
            Set<Boolean> bl = createBoolSet(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Character.class)) {
            Set<Character> bl = createCharSet(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Short.class)) {
            Set<Short> bl = createShortSet(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Integer.class)) {
            Set<Integer> bl = createIntSet(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Long.class)) {
            Set<Long> bl = createLongSet(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Float.class)) {
            Set<Float> bl = createFloatSet(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Double.class)) {
            Set<Double> bl = createDoubleSet(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(BigInteger.class)) {
            Set<BigInteger> bl = createBigIntegerSet(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(BigDecimal.class)) {
            Set<BigDecimal> bl = createBigDecimalSet(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        } else if (ptype.equals(Date.class)) {
            Set<Date> bl = createDateSet(values);
            ReflectionUtils.setObjectValue(source, field, bl);
        }
    }

    private static Set<Boolean> createBoolSet(List<String> values) {
        Set<Boolean> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Boolean.parseBoolean(value));
        }
        return nvalues;
    }

    private static Set<Character> createCharSet(List<String> values) {
        Set<Character> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(value.charAt(0));
        }
        return nvalues;
    }

    private static Set<Short> createShortSet(List<String> values) {
        Set<Short> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Short.parseShort(value));
        }
        return nvalues;
    }

    private static Set<Integer> createIntSet(List<String> values) {
        Set<Integer> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Integer.parseInt(value));
        }
        return nvalues;
    }

    private static Set<Long> createLongSet(List<String> values) {
        Set<Long> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Long.parseLong(value));
        }
        return nvalues;
    }

    private static Set<Float> createFloatSet(List<String> values) {
        Set<Float> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Float.parseFloat(value));
        }
        return nvalues;
    }

    private static Set<Double> createDoubleSet(List<String> values) {
        Set<Double> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Double.parseDouble(value));
        }
        return nvalues;
    }

    private static Set<BigInteger> createBigIntegerSet(List<String> values) {
        Set<BigInteger> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(new BigInteger(value));
        }
        return nvalues;
    }

    private static Set<BigDecimal> createBigDecimalSet(List<String> values) {
        Set<BigDecimal> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(new BigDecimal(value));
        }
        return nvalues;
    }

    private static Set<Date> createDateSet(List<String> values) {
        Set<Date> nvalues = new HashSet<>(values.size());
        SimpleDateFormat format = new SimpleDateFormat();
        try {
            for (String value : values) {
                Date dt = format.parse(value);
                nvalues.add(dt);
            }
            return nvalues;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
