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

import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.ObjectState;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ConfigPath(path = "connections")
@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public class ConnectionManager implements IConfigurable, Closeable {
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private Map<String, AbstractConnection> connections = new ConcurrentHashMap<>();
    @Setter(AccessLevel.NONE)
    private ObjectState state = new ObjectState();

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
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
                for(ConfigElementNode n : nodes) {
                    AbstractConnection<?> connection = readConnection((ConfigPathNode) n);
                    if (connection != null) {
                        connections.put(connection.name(), connection);
                    }
                }
            }
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
            String cname = ConfigUtils.getClassAttribute(node);
            if (Strings.isNullOrEmpty(cname)) {
                throw new ConfigurationException(String.format("Connection class attribute not found. [path=%s]",
                        node.getAbsolutePath()));
            }
            Class<? extends AbstractConnection<?>> type = (Class<? extends AbstractConnection<?>>) Class.forName(cname);
            AbstractConnection<?> connection = type.newInstance();
            connection.configure(node);

            return (AbstractConnection<T>) connection;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> AbstractConnection<T> connection(@Nonnull String name) throws DataStoreException {
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

    private static final ConnectionManager __instance = new ConnectionManager();

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
}
