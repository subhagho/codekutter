package com.codekutter.zconfig.common.model;


public interface ConfigVault {
    ConfigVault addPasscode(Configuration config, String passcode)
            throws Exception;

    String getPasscode(Configuration config) throws Exception;

    String decrypt(String data, Configuration config) throws Exception;
}
