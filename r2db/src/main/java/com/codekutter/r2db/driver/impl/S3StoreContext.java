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

package com.codekutter.r2db.driver.impl;

import com.codekutter.common.Context;

import javax.annotation.Nonnull;

public class S3StoreContext extends Context {
    public static final String CONTEXT_CONTINUE_KEY = "context.S3.continuationKey";

    public S3StoreContext() {
    }

    public S3StoreContext(@Nonnull Context source) {
        super(source);
    }

    public S3StoreContext continuationKey(@Nonnull String key) {
        setParam(CONTEXT_CONTINUE_KEY, key);
        return this;
    }

    public String continuationKey() {
        return getStringParam(CONTEXT_CONTINUE_KEY);
    }
}
