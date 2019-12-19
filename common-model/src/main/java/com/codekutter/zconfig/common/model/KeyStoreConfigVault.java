package com.codekutter.zconfig.common.model;


public class KeyStoreConfigVault implements ConfigVault {
    @Override
    public ConfigVault addPasscode(Configuration config, String passcode) throws Exception {
        return null;
    }

    @Override
    public String getPasscode(Configuration config) throws Exception {
        return null;
    }

    @Override
    public String decrypt(String data, Configuration config) throws Exception {
        return null;
    }
}
