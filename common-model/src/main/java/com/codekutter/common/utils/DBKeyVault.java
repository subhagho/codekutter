package com.codekutter.common.utils;

import com.codekutter.common.IKeyVault;
import com.codekutter.common.model.ModifiedBy;
import com.codekutter.common.model.utils.VaultRecord;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.EncryptedValue;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "db-key-vault")
public class DBKeyVault implements IKeyVault {
    @ConfigValue(name = "url", required = true)
    private String dbUrl;
    @ConfigValue(name = "username", required = true)
    private String dbUser;
    @ConfigValue(name = "password", required = true)
    private EncryptedValue dbPassword;
    @ConfigValue(name = "dbname", required = true)
    private String dbName;
    @ConfigAttribute(name = "driver", required = true)
    private String driver;
    @ConfigAttribute(name = "dialect", required = true)
    private String dialect;
    @ConfigValue(name = "vaultKey", required = true)
    private EncryptedValue vaultKey;
    @ConfigValue(name = "ivSpec", required = true)
    private String ivSpec;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Session session;

    public IKeyVault withSession(@Nonnull Session session) {
        this.session = session;
        return this;
    }

    @Override
    public IKeyVault addPasscode(@Nonnull String name, @Nonnull char[] key, Object... params) throws SecurityException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkArgument(params != null && params.length > 0);
        Preconditions.checkState(vaultKey != null);
        Preconditions.checkState(session != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(ivSpec));

        String userId = (String) params[0];
        if (Strings.isNullOrEmpty(userId))
            throw new SecurityException("Caller User ID not specified.");

        try {
            byte[] encrypted = CypherUtils.encrypt(new String(key).getBytes(StandardCharsets.UTF_8), vaultKey.getDecryptedValue(), ivSpec);
            Transaction tnx = session.beginTransaction();
            try {

                VaultRecord record = session.find(VaultRecord.class, name);
                if (record == null) {
                    record = new VaultRecord();
                    record.setKey(name);
                    record.setData(encrypted);
                    record.setCreatedBy(new ModifiedBy(userId));
                    record.setModifiedBy(record.getCreatedBy());
                } else {
                    record.setData(encrypted);
                    record.setModifiedBy(new ModifiedBy(userId));
                }
                session.save(record);
                tnx.commit();
            } catch (Throwable t) {
                tnx.rollback();
                throw t;
            }

        } catch (Exception e) {
            throw new SecurityException(e);
        }
        return this;
    }

    @Override
    public char[] getPasscode(@Nonnull String name) throws SecurityException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkState(vaultKey != null);
        Preconditions.checkState(session != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(ivSpec));

        VaultRecord record = session.find(VaultRecord.class, name);
        if (record == null) {
            throw new SecurityException(String.format("No record found for key. [name=%s]", name));
        }
        try {
            byte[] d = CypherUtils.decrypt(record.getData(), vaultKey.getDecryptedValue(), ivSpec);
            return new String(d, StandardCharsets.UTF_8).toCharArray();
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    @Override
    public byte[] decrypt(@Nonnull String data, @Nonnull String name, @Nonnull String iv) throws SecurityException {
        char[] p = getPasscode(name);
        if (p == null || p.length <= 0) {
            throw new SecurityException(String.format("Error getting password for key. [name=%s]", name));
        }
        try {
            return CypherUtils.decrypt(data, new String(p), iv);
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    @Override
    public byte[] decrypt(@Nonnull byte[] data, @Nonnull String name, @Nonnull String iv) throws SecurityException {
        char[] p = getPasscode(name);
        if (p == null || p.length <= 0) {
            throw new SecurityException(String.format("Error getting password for key. [name=%s]", name));
        }
        try {
            return CypherUtils.decrypt(data, new String(p), iv);
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    /**
     * Configure this type instance.
     * Note: Auto-configuration of this entity needs to happen prior to
     * configure method being called.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        if (session == null) {
            // TODO: Configure driver
        }
    }
}
