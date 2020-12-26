/*
 *  Copyright (2019) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.codekutter.common.stores.impl;

import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionException;
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ThreadCache;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.EncryptedValue;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.service.ServiceRegistry;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@Accessors(fluent = true)
public class HibernateConnection extends AbstractConnection<Session> {
    @Setter(AccessLevel.NONE)
    private SessionFactory sessionFactory = null;
    @ConfigValue(name = "config")
    private String hibernateConfigSource;
    @ConfigValue(name = "password", required = true)
    private EncryptedValue dbPassword;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private final ThreadCache<Session> threadCache = new ThreadCache<>();

    public HibernateConnection withSessionFactory(@Nonnull SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
        return this;
    }

    @Override
    public boolean hasTransactionSupport() {
        return true;
    }

    @Override
    public void close(@Nonnull Session connection) throws ConnectionException {
        try {
            if (connection.isOpen()) {
                connection.close();
            } else {
                LogUtils.warn(getClass(), "Connection already closed...");
            }

            Session cs = threadCache.remove();
            if (cs == null) {
                throw new ConnectionException("Connection not created via connection manager...", HibernateConnection.class);
            }
            if (!cs.equals(connection)) {
                throw new ConnectionException("Connection handle passed doesn't match cached connection.", HibernateConnection.class);
            }
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
        }
    }

    @Override
    public Session connection() throws ConnectionException {
        try {
            state().checkOpened();
            if (threadCache.contains()) {
                Session session = threadCache.get();
                if (session.isOpen()) return session;
            }
            synchronized (threadCache) {
                Session session = sessionFactory.openSession();
                return threadCache.put(session);
            }
        } catch (Throwable t) {
            throw new ConnectionException(t, getClass());
        }
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            if (!Strings.isNullOrEmpty(hibernateConfigSource)) {
                File cfg = new File(hibernateConfigSource);
                if (!cfg.exists()) {
                    throw new ConfigurationException(String.format("Hibernate configuration not found. [path=%s]", cfg.getAbsolutePath()));
                }
                Properties settings = new Properties();
                settings.setProperty(Environment.PASS, dbPassword.getDecryptedValue());
                sessionFactory = new Configuration().configure(cfg).addProperties(settings).buildSessionFactory();
            } else {
                AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
                if (!(cnode instanceof ConfigPathNode)) {
                    throw new ConfigurationException(String.format("Hibernate configuration settings not found. [node=%s]", node.getAbsolutePath()));
                }
                ConfigPathNode cp = (ConfigPathNode) cnode;

                HibernateConfig cfg = new HibernateConfig();
                ConfigurationAnnotationProcessor.readConfigAnnotations(HibernateConfig.class, cp, cfg);

                Configuration configuration = new Configuration();

                Properties settings = new Properties();
                settings.setProperty(Environment.DRIVER, cfg.driver);
                settings.setProperty(Environment.URL, cfg.dbUrl);
                settings.setProperty(Environment.USER, cfg.dbUser);
                settings.setProperty(Environment.PASS, dbPassword.getDecryptedValue());
                settings.setProperty(Environment.DIALECT, cfg.dialect);

                if (cfg.enableConnectionPool) {
                    if (cfg.poolMinSize < 0 || cfg.poolMaxSize <= 0 || cfg.poolMaxSize < cfg.poolMinSize) {
                        throw new ConfigurationException(
                                String.format("Invalid Pool Configuration : [min size=%d][max size=%d",
                                        cfg.poolMinSize, cfg.poolMaxSize));
                    }
                    settings.setProperty(HibernateConfig.CONFIG_C3P0_PREFIX + ".min_size", "" + cfg.poolMinSize);
                    settings.setProperty(HibernateConfig.CONFIG_C3P0_PREFIX + ".max_size", "" + cfg.poolMaxSize);
                    if (cfg.poolTimeout > 0)
                        settings.setProperty(HibernateConfig.CONFIG_C3P0_PREFIX + ".timeout", "" + cfg.poolTimeout);
                    if (cfg.poolConnectionCheck) {
                        settings.setProperty(HibernateConfig.CONFIG_C3P0_PREFIX + ".testConnectionOnCheckout", "true");
                    }
                }
                if (cfg.enableCaching) {
                    if (Strings.isNullOrEmpty(cfg.cacheConfig)) {
                        throw new ConfigurationException("Missing cache configuration file. ");
                    }
                    settings.setProperty(Environment.USE_SECOND_LEVEL_CACHE, "true");
                    settings.setProperty(Environment.CACHE_REGION_FACTORY, HibernateConfig.CACHE_FACTORY_CLASS);
                    if (cfg.enableQueryCaching)
                        settings.setProperty(Environment.USE_QUERY_CACHE, "true");
                    settings.setProperty(HibernateConfig.CACHE_CONFIG_FILE, cfg.cacheConfig);
                }
                if (cp.parmeters() != null) {
                    Map<String, ConfigValueNode> params = cp.parmeters().getKeyValues();
                    if (params != null && !params.isEmpty()) {
                        for (String key : params.keySet()) {
                            settings.setProperty(key, params.get(key).getValue());
                        }
                    }
                }
                configuration.setProperties(settings);

                if (supportedTypes() != null && !supportedTypes().isEmpty()) {
                    for (Class<?> cls : supportedTypes()) {
                        configuration.addAnnotatedClass(cls);
                    }
                }

                AbstractConfigNode hnode = cnode.find(HibernateConfig.CONFIG_HIBERNATE_PATH);
                if (hnode instanceof ConfigPathNode) {
                    if (((ConfigPathNode) hnode).parmeters() != null) {
                        Map<String, ConfigValueNode> params = ((ConfigPathNode) hnode).parmeters().getKeyValues();
                        if (params != null && !params.isEmpty()) {
                            for (String key : params.keySet()) {
                                String p = String.format("hibernate.%s", key);
                                settings.setProperty(p, params.get(key).getValue());
                                LogUtils.debug(getClass(), String.format("Added hibernate configuration. [param=%s][value=%s]",
                                        p, params.get(key).getValue()));
                            }
                        }
                    }
                }
                ServiceRegistry registry = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();
                sessionFactory = configuration.buildSessionFactory(registry);

                state().setState(EConnectionState.Open);
            }
        } catch (Exception ex) {
            state().setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (state().isOpen())
            state().setState(EConnectionState.Closed);
        threadCache.close();
        if (sessionFactory != null) {
            sessionFactory.close();
            sessionFactory = null;
        }
    }

    public Transaction startTransaction() throws ConnectionException {
        Session session = connection();
        Transaction tx = null;
        if (session.isJoinedToTransaction()) {
            tx = session.getTransaction();
        } else {
            tx = session.beginTransaction();
        }
        return tx;
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    @ConfigPath(path = "connection")
    public static class HibernateConfig {
        public static final String CACHE_FACTORY_CLASS = "org.hibernate.cache.ehcache.EhCacheRegionFactory";
        public static final String CACHE_CONFIG_FILE = "net.sf.ehcache.configurationResourceName";
        public static final String CONFIG_HIBERNATE_PATH = "hibernate";
        public static final String CONFIG_C3P0_PATH = "pool";
        public static final String CONFIG_C3P0_PREFIX = "hibernate.c3p0";
        private static final int DEFAULT_POOL_MIN_SIZE = 4;
        private static final int DEFAULT_POOL_MAX_SIZE = 8;
        private static final int DEFAULT_POOL_TIMEOUT = 1800;

        @ConfigValue(name = "url", required = true)
        private String dbUrl;
        @ConfigValue(name = "username", required = true)
        private String dbUser;
        @ConfigValue(name = "dbname")
        private String dbName;
        @ConfigAttribute(name = "driver", required = true)
        private String driver;
        @ConfigAttribute(name = "dialect", required = true)
        private String dialect;
        @ConfigValue(name = "enableCaching")
        private boolean enableCaching = false;
        @ConfigValue(name = "enableQueryCaching")
        private boolean enableQueryCaching = false;
        @ConfigValue(name = "cacheConfig")
        private String cacheConfig;
        @ConfigAttribute(name = "pool@enable")
        private boolean enableConnectionPool = false;
        @ConfigValue(name = "pool/minSize")
        private int poolMinSize = DEFAULT_POOL_MIN_SIZE;
        @ConfigValue(name = "pool/maxSize")
        private int poolMaxSize = DEFAULT_POOL_MAX_SIZE;
        @ConfigValue(name = "pool/timeout")
        private long poolTimeout = DEFAULT_POOL_TIMEOUT;
        @ConfigAttribute(name = "pool@check")
        private boolean poolConnectionCheck = true;
    }

}
