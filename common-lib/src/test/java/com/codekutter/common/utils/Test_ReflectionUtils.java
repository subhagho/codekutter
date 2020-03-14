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
 * Date: 3/2/19 7:38 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.utils;

import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListElementNode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

class Test_ReflectionUtils {
    @Test
    void isSuperType() {
        boolean ret = ReflectionUtils
                .isSuperType(AbstractConfigNode.class, ConfigListElementNode.class);
        assertTrue(ret);
    }

    @Test
    void implementsInterface() {
        boolean ret = ReflectionUtils
                .implementsInterface(Queue.class, ArrayBlockingQueue.class);
        assertTrue(ret);
    }

    @Test
    void getGenericListType() {
        try {
            Class<?> cls = TestList.class;
            Field f = cls.getDeclaredField("strings");
            assertNotNull(f);
            Class<?> t = ReflectionUtils.getGenericListType(f);
            assertNotNull(t);
            LogUtils.info(getClass(),
                    String.format("Field=%s", t.getCanonicalName()));
            f = cls.getDeclaredField("ints");
            assertNotNull(f);
            t = ReflectionUtils.getGenericListType(f);
            assertNotNull(t);
            LogUtils.info(getClass(),
                    String.format("Field=%s", t.getCanonicalName()));
        } catch (Exception e) {
            LogUtils.error(getClass(), e);
            fail(e.getLocalizedMessage());
        }
    }

    static class TestList {
        private List<String> strings;
        private ArrayList<Integer> ints;

        public List<String> getStrings() {
            return strings;
        }

        public void setStrings(List<String> strings) {
            this.strings = strings;
        }

        public ArrayList<Integer> getInts() {
            return ints;
        }
    }
}