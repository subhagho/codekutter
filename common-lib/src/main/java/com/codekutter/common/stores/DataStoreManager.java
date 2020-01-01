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

import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.annotations.IShardProvider;
import com.codekutter.common.stores.annotations.SchemaSharded;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ConfigPath(path = "dataStoreManager")
@SuppressWarnings("rawtypes")
public class DataStoreManager implements IConfigurable {
    public static final String CONFIG_NODE_CONNECTIONS = "connections";
    public static final String CONFIG_NODE_DATA_STORES = "dataStores";
    public static final String CONFIG_NODE_SHARDED_ENTITIES = "shardedEntities";

    @Getter
    @Setter
    @Accessors(fluent = true)
    @ConfigPath(path = "shard")
    public static final class ShardConfig {
        @ConfigAttribute(name = "provider")
        private Class<? extends IShardProvider> provider;
        @ConfigAttribute(name = "entityType", required = true)
        private Class<? extends IShardedEntity> entityType;
        @ConfigValue(name = "shards")
        private Map<Integer, String> shards = new HashMap<>();
    }

    private Map<String, AbstractConnection> connections = new HashMap<>();
    private Map<Class<? extends IEntity>, AbstractConnection> entityIndex = new HashMap<>();
    private Map<String, DataStoreConfig> dataStoreConfigs = new HashMap<>();
    private Map<Long, Map<String, AbstractDataStore>> initializedStores = new ConcurrentHashMap<>();
    private Map<Class<? extends IShardedEntity>, ShardConfig> shardConfigs = new HashMap<>();

    @SuppressWarnings("unchecked")
    public <T> AbstractConnection<T> getConnection(@Nonnull String name, @Nonnull Class<? extends T> type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (connections.containsKey(name)) {
            return (AbstractConnection<T>) connections.get(name);
        }
        return null;
    }


    @SuppressWarnings("rawtypes")
    public <T> AbstractConnection<T> getConnection(@Nonnull Class<? extends IEntity> type) {
        return getConnection(type, false);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> AbstractConnection<T> getConnection(@Nonnull Class<? extends IEntity> type, boolean checkSuperTypes) {
        Class<? extends IEntity> ct = type;
        while (true) {
            if (entityIndex.containsKey(ct)) {
                return (AbstractConnection<T>) entityIndex.get(ct);
            }
            if (checkSuperTypes) {
                Class<?> t = ct.getSuperclass();
                if (ReflectionUtils.implementsInterface(IEntity.class, t)) {
                    ct = (Class<? extends IEntity>) t;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        return null;
    }

    public <T> AbstractDataStore<T> getDataStore(@Nonnull String name, @Nonnull Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {
        try {
            DataStoreConfig config = dataStoreConfigs.get(name);
            if (config == null) {
                throw new DataStoreException(String.format("No configuration found for data store type. [type=%s]", storeType.getCanonicalName()));
            }
            if (!config.dataStoreClass().equals(storeType)) {
                throw new DataStoreException(String.format("Invalid Data Store class. [store=%s][expected=%s][configured=%s]", name, storeType.getCanonicalName(), config.dataStoreClass().getCanonicalName()));
            }
            AbstractDataStore<T> store = storeType.newInstance();
            store.name(config.name());
            store.withConfig(config).configure(this);

            return store;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T, E extends IEntity> AbstractDataStore<T> getDataStore(@Nonnull String name, @Nonnull Class<? extends AbstractDataStore<T>> storeType, Class<? extends E> type) throws DataStoreException {
        long threadId = Thread.currentThread().getId();
        AbstractDataStore<?> store = null;
        AbstractConnection<T> connection =
                (AbstractConnection<T>) entityIndex.get(type);
        if (connection == null) {
            throw new DataStoreException(String.format("No connection found for entity type. [type=%s]", type.getCanonicalName()));
        }
        if (initializedStores.containsKey(threadId)) {
            Map<String, AbstractDataStore> stores = initializedStores.get(threadId);
            if (stores.containsKey(name)) {
                store = stores.get(name);
            }
        }
        if (store == null) {
            store = getDataStore(name, storeType);
            if (initializedStores.containsKey(threadId)) {
                Map<String, AbstractDataStore> stores = initializedStores.get(threadId);
                stores.put(store.name(), store);
            } else {
                Map<String, AbstractDataStore> stores = new HashMap<>();
                stores.put(store.name(), store);
                initializedStores.put(threadId, stores);
            }
        }
        return (AbstractDataStore<T>) store;
    }


    @SuppressWarnings("rawtypes")
    public <T, E extends IShardedEntity> AbstractDataStore<T> getShards(@Nonnull Class<? extends AbstractDataStore<T>> storeType, Class<? extends E> type, Object shardKey) throws DataStoreException {
        try {
            if (type.isAnnotationPresent(SchemaSharded.class)) {
                ShardConfig config = shardConfigs.get(type);
                if (config != null) {
                    IShardProvider provider = null;
                    if (config.provider == null) {
                        SchemaSharded ss = type.getAnnotation(SchemaSharded.class);
                        Class<? extends IShardProvider> cls = ss.provider();
                        provider = cls.newInstance();
                    } else {
                        Class<? extends IShardProvider> cls = config.provider();
                        provider = cls.newInstance();
                    }
                    int shard = provider.withShardCount(config.shards.size()).getShard(shardKey);
                    String name = config.shards.get(shard);
                    if (Strings.isNullOrEmpty(name)) {
                        throw new DataStoreException(String.format("Shard instance not found. [type=%s][index=%d]", type.getCanonicalName(), shard));
                    }
                    return getDataStore(name, storeType, type);
                } else {
                    throw new DataStoreException(String.format("No Shard Config found. [type=%s]", type.getCanonicalName()));
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        AbstractConfigNode dnode = ConfigUtils.getPathNode(getClass(),
                (ConfigPathNode) node);
        if (dnode == null) {
            throw new ConfigurationException(String.format("DataStore Manager configuration node found. [node=%s]", node.getAbsolutePath()));
        }
        AbstractConfigNode cnode = dnode.find(CONFIG_NODE_CONNECTIONS);
        if (cnode != null) {
            if (cnode instanceof ConfigPathNode) {
                AbstractConfigNode csnode = ConfigUtils.getPathNode(AbstractConnection.class,
                        (ConfigPathNode) cnode);
                if (csnode instanceof ConfigPathNode) {
                    readConnection((ConfigPathNode) csnode);
                } else {
                    LogUtils.warn(getClass(), String.format("Invalid connection configuration. [node=%s]", cnode.getAbsolutePath()));
                }
            } else if (cnode instanceof ConfigListElementNode) {
                List<ConfigElementNode> nodes = ((ConfigListElementNode) cnode).getValues();
                if (nodes != null && !nodes.isEmpty()) {
                    for (ConfigElementNode nn : nodes) {
                        readConnection((ConfigPathNode) nn);
                    }
                }
            } else {
                throw new ConfigurationException(String.format("Invalid connection definitions. [node=%s]", cnode.getAbsolutePath()));
            }
        }

        cnode = dnode.find(CONFIG_NODE_DATA_STORES);
        if (cnode != null) {
            if (cnode instanceof ConfigPathNode) {
                AbstractConfigNode csnode = ConfigUtils.getPathNode(AbstractDataStore.class,
                        (ConfigPathNode) cnode);
                if (csnode instanceof ConfigPathNode) {
                    readDataStoreConfig((ConfigPathNode) csnode);
                } else {
                    throw new ConfigurationException(String.format("Invalid dataStore configuration. [node=%s]", cnode.getAbsolutePath()));
                }
            } else if (cnode instanceof ConfigListElementNode) {
                List<ConfigElementNode> nodes = ((ConfigListElementNode) cnode).getValues();
                if (nodes != null && !nodes.isEmpty()) {
                    for (ConfigElementNode nn : nodes) {
                        readDataStoreConfig((ConfigPathNode) nn);
                    }
                }
            } else {
                throw new ConfigurationException(String.format("Invalid dataStore definitions. [node=%s]", cnode.getAbsolutePath()));
            }
        }

        cnode = dnode.find(CONFIG_NODE_SHARDED_ENTITIES);
        if (cnode != null) {
            if (cnode instanceof ConfigPathNode) {
                AbstractConfigNode csnode = ConfigUtils.getPathNode(ShardConfig.class, (ConfigPathNode) cnode);
                if (csnode instanceof ConfigPathNode) {
                    readShardConfig((ConfigPathNode) csnode);
                } else {
                    throw new ConfigurationException(String.format("Invalid Shard configuration. [node=%s]", cnode.getAbsolutePath()));
                }
            } else if (cnode instanceof ConfigListElementNode) {
                List<ConfigElementNode> nodes = ((ConfigListElementNode) cnode).getValues();
                if (nodes != null && !nodes.isEmpty()) {
                    for (ConfigElementNode nn : nodes) {
                        readShardConfig((ConfigPathNode) nn);
                    }
                }
            } else {
                throw new ConfigurationException(String.format("Invalid Shard definitions. [node=%s]", cnode.getAbsolutePath()));
            }
        }
    }

    private void readShardConfig(ConfigPathNode node) throws ConfigurationException {
        AbstractConfigNode cnode = ConfigUtils.getPathNode(ShardConfig.class,  node);
        if (cnode != null) {
            ShardConfig config = ConfigurationAnnotationProcessor.readConfigAnnotations(ShardConfig.class, node);
            if (config == null) {
                throw new ConfigurationException(String.format("Error reading shard configuration. [node=%s]", cnode.getAbsolutePath()));
            }
            shardConfigs.put(config.entityType, config);
        } else {
            throw new ConfigurationException(String.format("Shard configuration not found. [node=%s]", node.getAbsolutePath()));
        }
    }

    @SuppressWarnings("rawtypes")
    private void readConnection(ConfigPathNode node) throws ConfigurationException {
        AbstractConfigNode cnode = ConfigUtils.getPathNode(AbstractConnection.class, node);
        if (!(cnode instanceof ConfigPathNode)) {
            throw new ConfigurationException(String.format("Invalid/NULL connection configuration. [node=%s]", node.getAbsolutePath()));
        }
        String type = ConfigUtils.getClassAttribute(cnode);
        if (Strings.isNullOrEmpty(type)) {
            throw new ConfigurationException(String.format("Invalid configuration: Class attribute not found. [node=%s]", cnode.getAbsolutePath()));
        }
        try {
            Class<?> cls = Class.forName(type);
            Object obj = cls.newInstance();
            if (!(obj instanceof AbstractConnection)) {
                throw new ConfigurationException(String.format("Invalid connection type. [type=%s]", cls.getCanonicalName()));
            }
            AbstractConnection<?> connection = (AbstractConnection<?>) obj;
            connection.configure(cnode);

            connections.put(connection.name(), connection);
            List<Class<? extends IEntity>> classes = connection.supportedTypes();
            if (classes != null && !classes.isEmpty()) {
                for (Class<? extends IEntity> entity : classes) {
                    entityIndex.put(entity, connection);
                }
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private void readDataStoreConfig(ConfigPathNode node) throws ConfigurationException {
        AbstractConfigNode cnode = ConfigUtils.getPathNode(DataStoreConfig.class, node);
        if (!(cnode instanceof ConfigPathNode)) {
            throw new ConfigurationException(String.format("Invalid/NULL data store configuration. [node=%s]", node.getAbsolutePath()));
        }
        String type = ConfigUtils.getClassAttribute(cnode);
        if (Strings.isNullOrEmpty(type)) {
            throw new ConfigurationException(String.format("Invalid configuration: Class attribute not found. [node=%s]", cnode.getAbsolutePath()));
        }
        try {
            Class<?> cls = Class.forName(type);
            Object obj = cls.newInstance();
            if (!(obj instanceof DataStoreConfig)) {
                throw new ConfigurationException(String.format("Invalid data store config type. [type=%s]", cls.getCanonicalName()));
            }
            DataStoreConfig config = (DataStoreConfig) obj;
            ConfigurationAnnotationProcessor.readConfigAnnotations(cls,
                    (ConfigPathNode) cnode, config);
            dataStoreConfigs.put(
                    config.name(), config);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

}
