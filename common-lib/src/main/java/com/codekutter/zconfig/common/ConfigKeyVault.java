package com.codekutter.zconfig.common;

import com.codekutter.common.IKeyVault;
import com.codekutter.common.utils.CypherUtils;
import com.codekutter.zconfig.common.model.Configuration;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;

public class ConfigKeyVault {
    private IKeyVault vault = null;

    public ConfigKeyVault withVault(@Nonnull IKeyVault vault) {
        this.vault = vault;
        return this;
    }

    public ConfigKeyVault save(@Nonnull String key, @Nonnull Configuration config) throws SecurityException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        Preconditions.checkState(vault != null);

        try {
            String name = String.format("%s:%s:%s", config.getApplicationGroup(), config.getApplication(), config.getName());
            vault.addPasscode(name, key.toCharArray());
        } catch (Exception e) {
            throw new SecurityException(e);
        }
        return this;
    }

    public String decrypt(@Nonnull String value, @Nonnull Configuration config) throws SecurityException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        Preconditions.checkState(vault != null);

        try {
            String name = String.format("%s:%s:%s", config.getApplicationGroup(), config.getApplication(), config.getName());

            String iv = getIVSpec(config);
            byte[] data = vault.decrypt(value, name, iv);
            if (data == null || data.length <= 0) {
                throw new SecurityException(String.format("No passcode returned for configuration. [name=%s]", name));
            }
            return new String(data, Charset.defaultCharset());
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    private String getIVSpec(Configuration config) throws Exception {
        return getIvSpec(config.getId(), config.getApplicationGroup(),
                config.getApplication(), config.getName(), config.getEncryptionHash());
    }

    public static String getIvSpec(@Nonnull String id, @Nonnull String group,
                                   @Nonnull String app, @Nonnull String name,
                                   @Nonnull String keyHash) throws Exception {
        StringBuffer buffer = new StringBuffer();
        buffer.append(id).append(group).append(app).append(name).append(keyHash);

        String spec = CypherUtils.getKeyHash(buffer.toString());
        return spec.substring(0, 16);
    }

    private static final ConfigKeyVault _instance = new ConfigKeyVault();

    public static ConfigKeyVault getInstance() {
        return _instance;
    }
}
