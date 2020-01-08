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

package com.codekutter.r2db.driver.model;

import com.amazonaws.services.s3.AmazonS3;
import com.codekutter.common.Context;

import javax.annotation.Nonnull;

public class S3FileContext extends Context {
    public static final String CONTEXT_S3_CLIENT = "context.S3.Client";
    public static final String CONTEXT_S3_BUCKET = "context.S3.Bucket";
    public static final String CONTEXT_S3_PATH = "context.S3.Path";

    public S3FileContext withClient(@Nonnull AmazonS3 client) {
        setParam(CONTEXT_S3_CLIENT, client);
        return this;
    }

    public AmazonS3 client() {
        return (AmazonS3) getParam(CONTEXT_S3_CLIENT);
    }

    public S3FileContext withBucket(@Nonnull String bucket) {
        setParam(CONTEXT_S3_BUCKET, bucket);
        return this;
    }

    public String bucket() {
        return (String) getParam(CONTEXT_S3_BUCKET);
    }

    public S3FileContext withPath(@Nonnull String path) {
        setParam(CONTEXT_S3_PATH, path);
        return this;
    }

    public String path() {
        return getStringParam(CONTEXT_S3_PATH);
    }
}

