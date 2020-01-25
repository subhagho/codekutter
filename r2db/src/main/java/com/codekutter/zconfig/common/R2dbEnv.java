package com.codekutter.zconfig.common;

import com.codekutter.common.auditing.AuditManager;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.r2db.driver.EntityManager;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

public class R2dbEnv extends ExtendedZConfigEnv {
    public static final String CONFIG_PATH_ENTITY_MANAGER = "/configuration/r2db";
    public static final String CONFIG_PATH_AUDIT_LOGGER = "/configuration/audit";

    private EntityManager entityManager = new EntityManager();

    public EntityManager getEntityManager() {
        return entityManager;
    }

    protected R2dbEnv(@Nonnull String configName) {
        super(configName);
    }

    /**
     * Perform post-initialisation tasks if any.
     *
     * @throws ConfigurationException
     */
    @Override
    public void postInit() throws ConfigurationException {
        super.postInit();
        AbstractConfigNode node = getConfiguration().getRootConfigNode().find(CONFIG_PATH_ENTITY_MANAGER);
        if (node instanceof ConfigPathNode) {
            entityManager.configure(node);
        } else {
            LogUtils.warn(getClass(), "Entity Manager configuration not set.");
        }

        node = getConfiguration().getRootConfigNode().find(CONFIG_PATH_AUDIT_LOGGER);
        if (node instanceof ConfigPathNode) {
            AuditManager.get().withDataStoreManager(entityManager.dataStoreManager());
            AuditManager.init(node);
        }
    }

    /**
     * Disposed this client environment instance.
     */
    @Override
    protected void dispose() {
        try {
            entityManager.close();
            ConnectionManager.dispose();
            super.dispose();
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
        }
    }

    /**
     * Get the instance of the client environment handle.
     *
     * @return - Client environment handle.
     * @throws EnvException
     */
    public static R2dbEnv env() throws EnvException {
        ZConfigEnv env = ZConfigEnv.env();
        if (env instanceof R2dbEnv) {
            return (R2dbEnv) env;
        }
        throw new EnvException(
                String.format("Env handle is not of client type. [type=%s]",
                        env.getClass().getCanonicalName()));
    }
}
