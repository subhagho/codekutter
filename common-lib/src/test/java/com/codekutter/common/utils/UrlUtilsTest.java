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

package com.codekutter.common.utils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.fail;

class UrlUtilsTest {

    @Test
    void replaceParams() {
        try {
            String url = "http://www.test.com/{id}/name/{value}-{id}";
            Map<String, String> params = new HashMap<>();
            params.put("id", UUID.randomUUID().toString());
            params.put("value", "VALUE-TEST");

            url = UrlUtils.replaceParams(url, params);

            LogUtils.debug(getClass(), url);
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex);
        }
    }
}