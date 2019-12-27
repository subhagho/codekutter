package com.codekutter.common.utils;

import com.codekutter.common.IKeyVault;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class KeyVaultManager {
    private static Map<String, IKeyVault> vaults = new HashMap<>();

    public static IKeyVault getVault(@Nonnull String name) {
        if (vaults.containsKey(name)) {
            return vaults.get(name);
        }
        return null;
    }

    public static void addVault(@Nonnull String name, @Nonnull IKeyVault vault) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        vaults.put(name, vault);
    }
}
