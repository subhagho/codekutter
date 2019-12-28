package com.codekutter.common.utils;

import com.codekutter.common.IKeyVault;
import com.codekutter.common.model.ModifiedBy;
import com.codekutter.common.model.utils.VaultRecord;
import com.codekutter.common.stores.impl.HibernateConnection;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.EncryptedValue;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
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
    @ConfigValue(name = "vaultKey")
    private EncryptedValue vaultKey;
    @ConfigValue(name = "ivSpec")
    private String ivSpec;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private HibernateConnection connection;

    public IKeyVault withConnection(@Nonnull HibernateConnection connection) {
        this.connection = connection;
        return this;
    }

    @Override
    public IKeyVault addPasscode(@Nonnull String name, @Nonnull char[] key, Object... params) throws SecurityException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkArgument(params != null && params.length > 0);
        Preconditions.checkState(vaultKey != null);
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(ivSpec));

        String userId = (String) params[0];
        if (Strings.isNullOrEmpty(userId))
            throw new SecurityException("Caller User ID not specified.");

        try {
            byte[] encrypted = CypherUtils.encrypt(new String(key).getBytes(StandardCharsets.UTF_8), vaultKey.getDecryptedValue(), ivSpec);

            Session session = connection.connection();
            try {
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
            } finally {
                connection.close();
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
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(ivSpec));

        try {
            Session session = connection.connection();
            try {
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
            } finally {
                connection.close();
            }
        } catch (Exception ex) {
            throw new SecurityException(ex);
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
        if (connection == null) {
            Preconditions.checkArgument(node instanceof ConfigPathNode);
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (cnode == null) {
                throw new ConfigurationException(String.format("Configuration node not found. [node=%s]", node.getAbsolutePath()));
            }
            connection = new HibernateConnection();
            connection.configure(cnode);
        }
    }
}
