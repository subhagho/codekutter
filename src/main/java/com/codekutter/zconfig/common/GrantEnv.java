package com.codekutter.zconfig.common;

import com.codekutter.zconfig.common.model.Version;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

public class GrantEnv extends ZConfigEnv {
    private GrantEnv(@Nonnull String configName) {
        super(configName);
    }

    @Override
    public void postInit() throws ConfigurationException {

    }

    /**
     * Setup the client environment using the passed configuration file.
     *
     * @param configfile - Configuration file (path) to read from.
     * @param version    - Configuration version (expected)
     * @throws ConfigurationException
     */
    public static void setup(@Nonnull String configfile, @Nonnull String version,
                             String password)
            throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configfile));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(version));

        try {
            ZConfigEnv.getEnvLock();
            try {
                ZConfigEnv env = ZConfigEnv.initialize(GrantEnv.class);
                if (env.getState() != EEnvState.Initialized) {
                    env.init(configfile, Version.parse(version), password);
                }
            } finally {
                ZConfigEnv.releaseEnvLock();
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }
}
