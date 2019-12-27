package com.codekutter.common;


import com.codekutter.zconfig.common.IConfigurable;

import javax.annotation.Nonnull;

public interface IKeyVault extends IConfigurable {
    IKeyVault addPasscode(@Nonnull String name, @Nonnull char[] key)
            throws SecurityException;

    char[] getPasscode(@Nonnull String name) throws SecurityException;

    byte[] decrypt(@Nonnull String data, @Nonnull String name, @Nonnull String iv) throws SecurityException;

    byte[] decrypt(@Nonnull byte[] data, @Nonnull String name, @Nonnull String iv) throws SecurityException;
}
