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

package com.codekutter.common;

import com.codekutter.common.utils.LogUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TimeWindowTest {

    @Test
    void parse() {
        try {
            String val = "100ss";
            TimeWindow window = TimeWindow.parse(val);
            assertEquals(100 * 1000, window.period());

            val = "32MI";
            window = TimeWindow.parse(val);
            assertEquals(32 * 60 * 1000, window.period());

            val = "23hh";
            window = TimeWindow.parse(val);
            assertEquals(23 * 60 * 60 * 1000, window.period());

            val = "3DD";
            window = TimeWindow.parse(val);
            assertEquals(3 * 24 * 60 * 60 * 1000, window.period());

            val = "2734683";
            int ival = Integer.parseInt(val);
            window = TimeWindow.parse(val);
            assertEquals(ival, window.period());

        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex);
        }
    }
}