/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
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

package com.codekutter.common.stores;

import com.codekutter.common.model.ConnectionConfig;
import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.ObjectState;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.common.utils.TypeUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.Session;

import javax.annotation.Nonnull;
import javax.persistence.Query;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ConfigPath(path = "connections")
@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public class ConnectionManager implements IConfigurable, Closeable {
    public static final String CONFIG_ATTR_SOURCE = "source";
    public static final String CONFIG_ATTR_CONN_CONFIG_TYPE = "configType";

    private static final ConnectionManager __instance = new ConnectionManager();
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private final Map<String, AbstractConnection> connections = new ConcurrentHashMap<>();
    @Setter(AccessLevel.NONE)
    private ObjectState state = new ObjectState();

    public static EConfigSource parseConfigSource(@Nonnull ConfigPathNode node) throws ConfigurationException {
        if (node.attributes() != null) {
            ConfigValueNode vn = node.attributes().getKeyValues().get(CONFIG_ATTR_SOURCE);
            if (vn != null) {
                String value = vn.getValue();
                return EConfigSource.valueOf(value);
            }
        }
        return EConfigSource.File;
    }

    private static Class<? extends ConnectionConfig> parseConnectionConfig(@Nonnull ConfigPathNode node) throws ConfigurationException {
        try {
            if (node.attributes() != null) {
                ConfigValueNode vn = node.attributes().getKeyValues().get(CONFIG_ATTR_CONN_CONFIG_TYPE);
                if (vn != null) {
                    String value = vn.getValue();
                    Class<? extends ConnectionConfig> cls = (Class<? extends ConnectionConfig>) Class.forName(value);
                    return cls;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public static ConnectionManager get() {
        return __instance;
    }

    public static void setup(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        if (__instance.state.getState() != EObjectState.Available) {
            __instance.configure(node);
        }
    }

    public static void dispose() {
        try {
            __instance.close();
        } catch (Exception ex) {
            LogUtils.error(ConnectionManager.class, ex);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> AbstractConnection<T> readConnection(@Nonnull ConfigPathNode inode) throws ConfigurationException {
        try {
            AbstractConfigNode node = ConfigUtils.getPathNode(AbstractConnection.class, inode);
            if (!(node instanceof ConfigPathNode)) {
                throw new ConfigurationException(String.format("Connection configuration node not found. [path=%s]",
                        inode.getAbsolutePath()));
            }
            AbstractConnection<T> connection = checkReference(inode);
            if (connection != null) {
                return connection;
            }
            connection = checkDbConfiguration(inode);
            if (connection != null) return connection;

            String cname = ConfigUtils.getClassAttribute(node);
            if (Strings.isNullOrEmpty(cname)) {
                throw new ConfigurationException(String.format("Connection class attribute not found. [path=%s]",
                        node.getAbsolutePath()));
            }
            Class<? extends AbstractConnection<T>> type = (Class<? extends AbstractConnection<T>>) Class.forName(cname);
            connection = TypeUtils.createInstance(type);
            connection.configure(node);

            return connection;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private <T> AbstractConnection<T> checkDbConfiguration(@Nonnull ConfigPathNode node) throws ConfigurationException {
        try {
            EConfigSource source = parseConfigSource(node);
            if (source == EConfigSource.Database) {
                String cls = ConfigUtils.getClassAttribute(node);
                if (Strings.isNullOrEmpty(cls)) {
                    throw ConfigurationException.propertyNotFoundException("class");
                }
                Class<? extends AbstractConnection<T>> type = (Class<? extends AbstractConnection<T>>) Class.forName(cls);
                String name = ConfigUtils.getNameAttribute(node);
                if (Strings.isNullOrEmpty(name)) {
                    throw ConfigurationException.propertyNotFoundException("name");
                }
                Class<? extends ConnectionConfig> configType = parseConnectionConfig(node);
                if (configType == null) {
                    throw ConfigurationException.propertyNotFoundException(CONFIG_ATTR_CONN_CONFIG_TYPE);
                }
                ConfigPath path = AbstractConnection.class.getAnnotation(ConfigPath.class);
                String sp = String.format("./%s", path.path());
                AbstractConfigNode cnode = node.find(sp);
                if (!(cnode instanceof ConfigPathNode)) {
                    throw new ConfigurationException(
                            String.format("DB Connection definition not found. [path=%s]", node.getAbsolutePath()));
                }
                AbstractConnection<Session> connection = readConnection((ConfigPathNode) cnode);
                if (connection == null) {
                    throw new ConfigurationException(
                            String.format("Error reading connection. [path=%s]", cnode.getAbsolutePath()));
                }
                Session session = connection.connection();
                try {
                    AbstractConnection<T> rc = readConnection(configType, type, session, name);
                    if (rc == null) {
                        throw new ConfigurationException(
                                String.format("Error reading connection from DB. [path=%s]", node.getAbsolutePath()));
                    }
                    return rc;
                } finally {
                    connection.close(session);
                }
            }
            return null;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private <T> AbstractConnection<T> checkReference(@Nonnull ConfigPathNode node) throws ConfigurationException {
        try {
            String ref = ConfigUtils.getReferenceAttribute(node);
            if (!Strings.isNullOrEmpty(ref)) {
                String cls = ConfigUtils.getClassAttribute(node);
                if (Strings.isNullOrEmpty(cls)) {
                    throw ConfigurationException.propertyNotFoundException("class");
                }
                Class<? extends AbstractConnection> type = (Class<? extends AbstractConnection>) Class.forName(cls);
                if (connections.containsKey(ref)) {
                    AbstractConnection<?> connection = connections.get(ref);
                    if (!ReflectionUtils.isSuperType(type, connection.getClass())) {
                        throw new ConfigurationException(
                                String.format("Connection type mismatch. [name=%s][expected=%s][actual=%s]",
                                        ref, type.getCanonicalName(), connection.getClass().getCanonicalName()));
                    }
                    return (AbstractConnection<T>) connection;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private <T, C extends ConnectionConfig> AbstractConnection<T> readConnection(@Nonnull Class<? extends C> configType,
                                                                                 @Nonnull Class<? extends AbstractConnection<T>> connectionType,
                                                                                 @Nonnull Session session,
                                                                                 String name) throws ConfigurationException {
        try {
            String qstr = String.format("FROM %s WHERE name = :p_name", configType.getCanonicalName());
            Query query = session.createQuery(qstr);
            query.setParameter("p_name", name);
            List<ConnectionConfig> configs = query.getResultList();
            if (configs != null && !configs.isEmpty()) {
                ConnectionConfig config = configs.get(0);
                config.load();

                AbstractConnection<T> connection = TypeUtils.createInstance(connectionType);
                connection.configure(config);
                if (connection.supportedTypes() == null)
                    connection.supportedTypes(config.getSupportedTypes());
                else if (config.getSupportedTypes() != null) {
                    connection.supportedTypes().addAll(config.getSupportedTypes());
                }

                return connection;
            }
            return null;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public <T, C extends ConnectionConfig> List<AbstractConnection<T>> readConnections(@Nonnull Class<? extends C> configType,
                                                                                       @Nonnull Session session,
                                                                                       String filter) throws ConfigurationException {
        try {
            String qstr = String.format("FROM %s", configType.getCanonicalName());
            if (!Strings.isNullOrEmpty(filter)) {
                qstr = String.format("%s WHERE (%s)", qstr, filter);
            }
            Query query = session.createQuery(qstr);
            List<ConnectionConfig> configs = query.getResultList();
            if (configs != null && !configs.isEmpty()) {
                List<AbstractConnection<T>> conns = new ArrayList<>();
                synchronized (connections) {
                    for (ConnectionConfig config : configs) {
                        config.load();

                        AbstractConnection<T> connection = TypeUtils.createInstance(config.getConnectionType());
                        connection.configure(config);
                        if (connection.supportedTypes() == null)
                            connection.supportedTypes(config.getSupportedTypes());
                        else if (config.getSupportedTypes() != null) {
                            connection.supportedTypes().addAll(config.getSupportedTypes());
                        }
                        connections.put(config.getName(), connection);
                        conns.add(connection);
                    }
                }
                return conns;
            }
            return null;
        } catch (Throwable t) {
            throw new ConfigurationException(t);
        }
    }

    /**
     * Configure this type instance.
     *
     * @param inode - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode inode) throws ConfigurationException {
        Preconditions.checkArgument(inode instanceof ConfigPathNode);
        AbstractConfigNode node = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) inode);
        if (node instanceof ConfigPathNode) {
            AbstractConfigNode cnode = ConfigUtils.getPathNode(AbstractConnection.class, (ConfigPathNode) node);
            if (!(cnode instanceof ConfigPathNode)) {
                throw new ConfigurationException(String.format("Invalid connection configuration. [node=%s]", node.getAbsolutePath()));
            }
            AbstractConnection<?> connection = readConnection((ConfigPathNode) cnode);
            if (connection != null) {
                connections.put(connection.name(), connection);
            }
        } else if (node instanceof ConfigListElementNode) {
            List<ConfigElementNode> nodes = ((ConfigListElementNode) node).getValues();
            if (nodes != null && !nodes.isEmpty()) {
                for (ConfigElementNode n : nodes) {
                    AbstractConnection<?> connection = readConnection((ConfigPathNode) n);
                    if (connection != null) {
                        connections.put(connection.name(), connection);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T> AbstractConnection<T> connection(@Nonnull String name, @Nonnull Class<? extends T> type) throws DataStoreException {
        if (connections.containsKey(name)) {
            return connections.get(name);
        }
        throw new DataStoreException(String.format("Connection not found. [name=%s]", name));
    }

    @Override
    public void close() throws IOException {
        if (state.getState() == EObjectState.Available) {
            state.setState(EObjectState.Disposed);
        }
        if (!connections.isEmpty()) {
            for (AbstractConnection<?> connection : connections.values()) {
                connection.close();
            }
            connections.clear();
        }
    }
}
