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
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.common.utils.ConfigUtils;
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
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.service.ServiceRegistry;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@Accessors(fluent = true)
public class HibernateConnection extends AbstractConnection<Session> {
    @Getter
    @Setter
    @Accessors(fluent = true)
    @ConfigPath(path = "settings")
    public static class HibernateConfig {
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
        @ConfigValue(name = "classes")
        private List<String> classes;
    }

    private static SessionFactory sessionFactory = null;

    @ConfigValue(name = "config")
    private String hibernateConfig;
    @ConfigValue(name = "password", required = true)
    private EncryptedValue dbPassword;

    @Override
    public boolean hasTransactionSupport() {
        return true;
    }

    @Override
    public Session connection() {
        if (super.connection() == null) {
            connection(sessionFactory.openSession());
        }
        return super.connection();
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            if (!Strings.isNullOrEmpty(hibernateConfig)) {
                File cfg = new File(hibernateConfig);
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

                if (cp.parmeters() != null) {
                    Map<String, ConfigValueNode> params = cp.parmeters().getKeyValues();
                    if (params != null && !params.isEmpty()) {
                        for (String key : params.keySet()) {
                            settings.setProperty(key, params.get(key).getValue());
                        }
                    }
                }
                configuration.setProperties(settings);

                if (cfg.classes != null && !cfg.classes.isEmpty()) {
                    for (String cls : cfg.classes) {
                        Class<?> c = Class.forName(cls);
                        configuration.addAnnotatedClass(c);
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
        if (connection() != null) {
            if (connection().isOpen()) {
                state().setState(EConnectionState.Closed);
                connection().close();
            }
            connection(null);
        }
    }

}
