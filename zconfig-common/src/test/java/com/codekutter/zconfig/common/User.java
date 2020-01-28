package com.codekutter.zconfig.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.security.Principal;

public class User implements Principal {
    private String name;

    public User(@Nonnull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
