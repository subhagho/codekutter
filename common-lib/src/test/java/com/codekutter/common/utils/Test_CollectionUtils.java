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
 * Date: 9/2/19 4:56 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.utils;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class Test_CollectionUtils {


    @Test
    void setListValues() {
        try {
            ListClass listClass = new ListClass();

            Random rr = new Random(System.currentTimeMillis());
            List<String> iStrings = new ArrayList<>();
            for (int ii = 0; ii < 10; ii++) {
                iStrings.add(String.valueOf(rr.nextInt()));
            }
            List<String> bdStrings = new ArrayList<>();
            for (int ii = 0; ii < 10; ii++) {
                bdStrings.add(String.valueOf(rr.nextDouble()));
            }
            List<String> dStrings = new ArrayList<>();
            SimpleDateFormat df = new SimpleDateFormat();
            for (int ii = 0; ii < 5; ii++) {
                Date dt = new Date(rr.nextLong());
                dStrings.add(df.format(dt));
            }

            Field f = ListClass.class.getDeclaredField("intSet");
            assertNotNull(f);
            CollectionUtils.setListValues(listClass, f, iStrings);

            f = ListClass.class.getDeclaredField("dateSet");
            assertNotNull(f);
            CollectionUtils.setListValues(listClass, f, dStrings);

            f = ListClass.class.getDeclaredField("bigDecimalSet");
            assertNotNull(f);
            CollectionUtils.setListValues(listClass, f, bdStrings);

            LogUtils.debug(getClass(), listClass);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t.getLocalizedMessage());
        }
    }

    @Test
    void setSetValues() {
        try {
            SetClass setClass = new SetClass();

            Random rr = new Random(System.currentTimeMillis());
            List<String> lStrings = new ArrayList<>();
            for (int ii = 0; ii < 10; ii++) {
                lStrings.add(String.valueOf(rr.nextLong()));
            }
            List<String> bdStrings = new ArrayList<>();
            for (int ii = 0; ii < 10; ii++) {
                bdStrings.add(String.valueOf(rr.nextDouble()));
            }
            List<String> sStrings = new ArrayList<>();
            for (int ii = 0; ii < 5; ii++) {
                sStrings.add(UUID.randomUUID().toString());
            }

            Field f = SetClass.class.getDeclaredField("longSet");
            assertNotNull(f);
            CollectionUtils.setSetValues(setClass, f, lStrings);

            f = SetClass.class.getDeclaredField("stringSet");
            assertNotNull(f);
            CollectionUtils.setSetValues(setClass, f, sStrings);

            f = SetClass.class.getDeclaredField("bigDecimalSet");
            assertNotNull(f);
            CollectionUtils.setSetValues(setClass, f, bdStrings);

            LogUtils.debug(getClass(), setClass);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t.getLocalizedMessage());
        }
    }
}