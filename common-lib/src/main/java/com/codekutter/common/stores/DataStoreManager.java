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
import com.codekutter.common.utils.*;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.Session;

import javax.annotation.Nonnull;
import javax.persistence.Query;
import java.io.IOException;
import java.util.*;

@ConfigPath(path = "dataStoreManager")
@SuppressWarnings("rawtypes")
public class DataStoreManager implements IConfigurable {
    public static final String CONFIG_NODE_DATA_STORES = "dataStores";
    public static final String CONFIG_NODE_SHARDED_ENTITIES = "shardedEntities";
    private final Map<Class<? extends IEntity>, Map<Class<? extends AbstractDataStore>, DataStoreConfig>> entityIndex = new HashMap<>();
    private final Map<String, DataStoreConfig> dataStoreConfigs = new HashMap<>();
    private final Map<Class<? extends IShardedEntity>, ShardConfig> shardConfigs = new HashMap<>();
    private final MapThreadCache<String, AbstractDataStore> openedStores = new MapThreadCache<>();

    public boolean isTypeSupported(@Nonnull Class<?> type) {
        if (ReflectionUtils.implementsInterface(IEntity.class, type)) {
            return entityIndex.containsKey(type);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public <T> AbstractConnection<T> getConnection(@Nonnull String name, Class<? extends T> type) throws DataStoreException {
        return ConnectionManager.get().connection(name, type);
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

    public <T> AbstractDataStore<T> getDataStore(@Nonnull String name,
                                                 @Nonnull Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException {
        return getDataStore(name, storeType, true);
    }

    public <T> AbstractDataStore<T> getDataStore(@Nonnull String name,
                                                 @Nonnull Class<? extends AbstractDataStore<T>> storeType,
                                                 boolean add) throws DataStoreException {
        try {
            DataStoreConfig config = dataStoreConfigs.get(name);
            if (config == null) {
                throw new DataStoreException(String.format("No configuration found for data store type. [type=%s]", storeType.getCanonicalName()));
            }
            if (!config.getDataStoreClass().equals(storeType)) {
                throw new DataStoreException(String.format("Invalid Data Store class. [store=%s][expected=%s][configured=%s]", name, storeType.getCanonicalName(), config.getDataStoreClass().getCanonicalName()));
            }
            return getDataStore(config, storeType, add);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <T, E extends IEntity> AbstractDataStore<T> getDataStore(@Nonnull Class<? extends AbstractDataStore<T>> storeType,
                                                                    Class<? extends E> type) throws DataStoreException {
        return getDataStore(storeType, type, true);
    }

    @SuppressWarnings({"rawtypes"})
    public <T, E extends IEntity> AbstractDataStore<T> getDataStore(@Nonnull Class<? extends AbstractDataStore<T>> storeType,
                                                                    Class<? extends E> type,
                                                                    boolean add) throws DataStoreException {
        Map<Class<? extends AbstractDataStore>, DataStoreConfig> configs = entityIndex.get(type);
        if (configs == null) {
            throw new DataStoreException(String.format("No data store found for entity type. [type=%s]", type.getCanonicalName()));
        }
        DataStoreConfig config = configs.get(storeType);
        if (config == null) {
            throw new DataStoreException(String.format("No data store found. [type=%s][store type=%s]", type.getCanonicalName(), storeType.getCanonicalName()));
        }

        try {
            return getDataStore(config, storeType, add);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> AbstractDataStore<T> getDataStore(DataStoreConfig config,
                                                  Class<? extends AbstractDataStore<T>> storeType,
                                                  boolean add) throws DataStoreException {
        Map<String, AbstractDataStore> stores = null;
        if (openedStores.containsThread()) {
            stores = openedStores.get();
            if (stores.containsKey(config.getName())) {
                return stores.get(config.getName());
            }
        } else if (!add) {
            return null;
        }

        try {
            AbstractDataStore<T> store = TypeUtils.createInstance(storeType);
            store.name(config.getName());
            store.withConfig(config).configure(this);
            openedStores.put(store.name(), store);

            return store;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings("rawtypes")
    public <T, E extends IShardedEntity> AbstractDataStore<T> getShard(@Nonnull Class<? extends AbstractDataStore<T>> storeType,
                                                                       @Nonnull Class<? extends E> type,
                                                                       Object shardKey) throws DataStoreException {
        try {
            if (type.isAnnotationPresent(SchemaSharded.class)) {
                ShardConfig config = shardConfigs.get(type);
                if (config != null) {
                    IShardProvider provider = null;
                    if (config.provider == null) {
                        SchemaSharded ss = type.getAnnotation(SchemaSharded.class);
                        Class<? extends IShardProvider> cls = ss.provider();
                        provider = TypeUtils.createInstance(cls);
                    } else {
                        Class<? extends IShardProvider> cls = config.provider();
                        provider = TypeUtils.createInstance(cls);
                    }
                    int shard = provider.withShardCount(config.shards.size()).getShard(shardKey);
                    String name = config.shards.get(shard);
                    if (Strings.isNullOrEmpty(name)) {
                        throw new DataStoreException(String.format("Shard instance not found. [type=%s][index=%d]", type.getCanonicalName(), shard));
                    }
                    return getDataStore(name, storeType);
                } else {
                    throw new DataStoreException(String.format("No Shard Config found. [type=%s]", type.getCanonicalName()));
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <T, E extends IShardedEntity> List<AbstractDataStore<T>> getShards(@Nonnull Class<? extends AbstractDataStore<T>> storeType,
                                                                              @Nonnull Class<? extends E> type) throws DataStoreException {
        try {
            if (type.isAnnotationPresent(SchemaSharded.class)) {
                List<AbstractDataStore<T>> stores = null;

                ShardConfig config = shardConfigs.get(type);
                if (config != null) {
                    stores = new ArrayList<>();
                    for (int shard : config.shards.keySet()) {
                        String name = config.shards.get(shard);
                        if (Strings.isNullOrEmpty(name)) {
                            throw new DataStoreException(String.format("Shard instance not found. [type=%s][index=%d]", type.getCanonicalName(), shard));
                        }
                        stores.add(getDataStore(name, storeType));
                    }
                    return stores;
                } else {
                    throw new DataStoreException(String.format("No Shard Config found. [type=%s]", type.getCanonicalName()));
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public void commit() throws DataStoreException {
        try {
            if (openedStores.containsThread()) {
                Map<String, AbstractDataStore> stores = openedStores.get();
                for (String name : stores.keySet()) {
                    AbstractDataStore<?> store = stores.get(name);
                    if (store.auditLogger() != null) {
                        store.auditLogger().flush();
                    }
                }
                for (String name : stores.keySet()) {
                    AbstractDataStore<?> store = stores.get(name);
                    if (store instanceof TransactionDataStore) {
                        if (((TransactionDataStore) store).isInTransaction()) {
                            ((TransactionDataStore) store).commit();
                        }
                    }
                }
            }
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public void rollback() throws DataStoreException {
        try {
            if (openedStores.containsThread()) {
                Map<String, AbstractDataStore> stores = openedStores.get();
                for (String name : stores.keySet()) {
                    AbstractDataStore<?> store = stores.get(name);
                    if (store.auditLogger() != null) {
                        store.auditLogger().discard();
                    }
                }
                for (String name : stores.keySet()) {
                    AbstractDataStore<?> store = stores.get(name);
                    if (store instanceof TransactionDataStore) {
                        if (((TransactionDataStore) store).isInTransaction()) {
                            ((TransactionDataStore) store).rollback();
                        }
                    }
                }
            }
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public void closeStores() throws DataStoreException {
        try {
            if (openedStores.containsThread()) {
                Map<String, AbstractDataStore> stores = openedStores.get();
                List<AbstractDataStore> storeList = new ArrayList<>();
                for (String name : stores.keySet()) {
                    AbstractDataStore<?> store = stores.get(name);
                    if (store.auditLogger() != null) store.auditLogger().discard();
                    if (store instanceof TransactionDataStore) {
                        if (((TransactionDataStore) store).isInTransaction()) {
                            LogUtils.error(getClass(), String.format("Store has pending transactions, rolling back. [name=%s][thread id=%d]",
                                    store.name(), Thread.currentThread().getId()));
                            ((TransactionDataStore) store).rollback();
                        }
                    }
                    storeList.add(store);
                }
                for (AbstractDataStore store : storeList) {
                    try {
                        store.close();
                    } catch (IOException e) {
                        LogUtils.error(getClass(), e);
                    }
                }
                openedStores.clear();
                storeList.clear();
            }
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public void close(@Nonnull AbstractDataStore dataStore) throws DataStoreException {
        try {
            if (openedStores.containsThread()) {
                Map<String, AbstractDataStore> stores = openedStores.get();
                if (stores.containsKey(dataStore.name())) {
                    if (dataStore.auditLogger() != null) {
                        dataStore.auditLogger().discard();
                    }
                    if (dataStore instanceof TransactionDataStore) {
                        TransactionDataStore ts = (TransactionDataStore) dataStore;
                        if (ts.isInTransaction()) {
                            LogUtils.error(getClass(), String.format("Data Store has un-committed transaction. [name=%s][thread=%d]",
                                    dataStore.name(), Thread.currentThread().getId()));
                            ts.rollback();
                        }
                    }
                    openedStores.remove(dataStore.name());
                }
            }
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

        AbstractConfigNode cnode = dnode.find(CONFIG_NODE_DATA_STORES);
        if (cnode != null) {
            if (cnode instanceof ConfigPathNode) {
                AbstractConfigNode csnode = ConfigUtils.getPathNode(DataStoreConfig.class,
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
        AbstractConfigNode cnode = ConfigUtils.getPathNode(ShardConfig.class, node);
        if (cnode instanceof ConfigPathNode) {
            ShardConfig config = ConfigurationAnnotationProcessor.readConfigAnnotations(ShardConfig.class, node);
            if (config == null) {
                throw new ConfigurationException(String.format("Error reading shard configuration. [node=%s]", cnode.getAbsolutePath()));
            }
            ConfigParametersNode params = ((ConfigPathNode) cnode).parmeters();
            if (params == null || params.isEmpty()) {
                throw new ConfigurationException(String.format("Shard segments not defined. [node=%s]", cnode.getAbsolutePath()));
            }
            config.shards = new HashMap<>();
            for (String key : params.getKeyValues().keySet()) {
                ConfigValueNode cv = params.getValue(key);
                config.shards.put(Integer.parseInt(key), cv.getValue());
            }
            shardConfigs.put(config.entityType, config);
        } else {
            throw new ConfigurationException(String.format("Shard configuration not found. [node=%s]", node.getAbsolutePath()));
        }
    }

    public <T> Set<String> readDynamicDConfig(@Nonnull Session session,
                                              @Nonnull Class<? extends AbstractDataStore<T>> dataStoreType,
                                              @Nonnull Class<? extends DataStoreConfig> configType,
                                              String filter) throws DataStoreException {
        try {
            String qstr = String.format("FROM %s", configType.getCanonicalName());
            if (!Strings.isNullOrEmpty(filter)) {
                qstr = String.format("%s WHERE (%s)", qstr, filter);
            }
            Query query = session.createQuery(qstr);
            List<DataStoreConfig> configs = query.getResultList();
            if (configs != null && !configs.isEmpty()) {
                Set<String> dataStores = new HashSet<>();
                synchronized (dataStoreConfigs) {
                    for (DataStoreConfig config : configs) {
                        if (openedStores.containsKey(config.getName())) {
                            throw new DataStoreException(
                                    String.format("Store already opened by current thread. [name=%s][type=%s]",
                                            config.getName(), dataStoreType.getCanonicalName()));
                        }
                        config.postLoad();
                        loadDataStoreConfig(config);
                        dataStores.add(config.getName());
                    }
                }
                return dataStores;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private void loadDataStoreConfig(DataStoreConfig config) throws ConfigurationException {
        try {
            dataStoreConfigs.put(
                    config.getName(), config);
            AbstractConnection<?> connection = ConnectionManager.get().connection(config.getConnectionName(), config.getConnectionType());
            if (connection == null) {
                throw new ConfigurationException(String.format("No connection found. [store=%s][connection=%s]", config.getName(), config.getConnectionName()));
            }
            if (connection.supportedTypes() != null && !connection.supportedTypes().isEmpty()) {
                for (Class<?> t : connection.supportedTypes()) {
                    if (ReflectionUtils.implementsInterface(IEntity.class, t)) {
                        Map<Class<? extends AbstractDataStore>, DataStoreConfig> ec = entityIndex.get(t);
                        if (ec == null) {
                            ec = new HashMap<>();
                            entityIndex.put((Class<? extends IEntity>) t, ec);
                        }
                        ec.put(config.getDataStoreClass(), config);
                    }
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
            Class<? extends DataStoreConfig> cls = (Class<? extends DataStoreConfig>) Class.forName(type);
            DataStoreConfig config = TypeUtils.createInstance(cls);
            ConfigurationAnnotationProcessor.readConfigAnnotations(cls,
                    (ConfigPathNode) cnode, config);
            loadDataStoreConfig(config);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    @ConfigPath(path = "shard")
    public static final class ShardConfig {
        @ConfigAttribute(name = "provider")
        private Class<? extends IShardProvider> provider;
        @ConfigAttribute(name = "entityType", required = true)
        private Class<? extends IShardedEntity> entityType;
        private Map<Integer, String> shards = new HashMap<>();
    }
}
