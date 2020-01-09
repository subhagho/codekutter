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
