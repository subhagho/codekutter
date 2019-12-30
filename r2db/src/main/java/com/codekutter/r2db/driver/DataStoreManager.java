package com.codekutter.r2db.driver;

import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ConfigPath(path = "dataStoreManager")
public class DataStoreManager implements IConfigurable {
    public static final String CONFIG_NODE_CONNECTIONS = "connections";
    public static final String CONFIG_NODE_DATA_STORES = "dataStores";

    private Map<String, AbstractConnection<?>> connections = new HashMap<>();
    @SuppressWarnings("rawtypes")
    private Map<Class<? extends IEntity>, AbstractConnection<?>> entityIndex = new HashMap<>();
    private Map<Class<? extends AbstractDataStore<?>>, DataStoreConfig> dataStoreConfigs = new HashMap<>();
    private Map<Long, Map<AbstractConnection<?>, AbstractDataStore<?>>> initializedStores = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public <T> AbstractConnection<T> getDataSource(@Nonnull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (connections.containsKey(name)) {
            return (AbstractConnection<T>) connections.get(name);
        }
        return null;
    }


    @SuppressWarnings("rawtypes")
    public <T> AbstractConnection<T> getDataSource(@Nonnull Class<? extends IEntity> type) {
        return getDataSource(type, false);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> AbstractConnection<T> getDataSource(@Nonnull Class<? extends IEntity> type, boolean checkSuperTypes) {
        Class<? extends IEntity> ct = type;
        while(true) {
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> AbstractDataStore<T> getDataStore(@Nonnull Class<? extends AbstractDataStore<T>> storeType) throws DataStoreException{
        try {
            AbstractDataStore<T> store = storeType.newInstance();
            DataStoreConfig config = dataStoreConfigs.get(storeType);
            if (config == null) {
                throw new DataStoreException(String.format("No configuration found for data store type. [type=%s]", storeType.getCanonicalName()));
            }
            AbstractConnection<T> connection =
                    (AbstractConnection<T>) connections.get(config.connectionName());
            if (connection == null) {
                throw new DataStoreException(String.format("No connection found for name. [name=%s]", config.connectionName()));
            }
            store.withConfig(config).withConnection(connection).configure();

            return store;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T, E extends IEntity> AbstractDataStore<T> getDataStore(@Nonnull Class<? extends AbstractDataStore<T>> storeType, Class<? extends E> type) throws DataStoreException {
        long threadId = Thread.currentThread().getId();
        AbstractDataStore<?> store = null;
        AbstractConnection<T> connection =
                (AbstractConnection<T>) entityIndex.get(type);
        if (connection == null) {
            throw new DataStoreException(String.format("No connection found for entity type. [type=%s]", type.getCanonicalName()));
        }
        if (initializedStores.containsKey(threadId)) {
            Map<AbstractConnection<?>, AbstractDataStore<?>> stores = initializedStores.get(threadId);
            if (stores.containsKey(connection)) {
                store = stores.get(connection);
            }
        }
        if (store == null) {
            store = getDataStore(storeType);
            if (initializedStores.containsKey(threadId)) {
                Map<AbstractConnection<?>, AbstractDataStore<?>> stores = initializedStores.get(threadId);
                stores.put(connection, store);
            } else {
                Map<AbstractConnection<?>, AbstractDataStore<?>> stores = new HashMap<>();
                stores.put(connection, store);
                initializedStores.put(threadId, stores);
            }
        }
        return (AbstractDataStore<T>) store;
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
                for(ConfigElementNode nn : nodes) {
                    readConnection((ConfigPathNode) nn);
                }
            }
        } else {
            throw new ConfigurationException(String.format("Invalid connection definitions. [node=%s]", cnode.getAbsolutePath()));
        }

        cnode = dnode.find(CONFIG_NODE_DATA_STORES);
        if (cnode instanceof ConfigPathNode) {
            AbstractConfigNode csnode = ConfigUtils.getPathNode(AbstractDataStore.class,
                                                                (ConfigPathNode) cnode);
            if (csnode instanceof ConfigPathNode) {
                readDataStoreConfig((ConfigPathNode) csnode);
            } else {
                LogUtils.warn(getClass(), String.format("Invalid dataStore configuration. [node=%s]", cnode.getAbsolutePath()));
            }
        } else if (cnode instanceof ConfigListElementNode) {
            List<ConfigElementNode> nodes = ((ConfigListElementNode) cnode).getValues();
            if (nodes != null && !nodes.isEmpty()) {
                for(ConfigElementNode nn : nodes) {
                    readDataStoreConfig((ConfigPathNode) nn);
                }
            }
        } else {
            throw new ConfigurationException(String.format("Invalid dataStore definitions. [node=%s]", cnode.getAbsolutePath()));
        }
    }

    private void readConnection(ConfigPathNode node) throws ConfigurationException {

    }

    private void readDataStoreConfig(ConfigPathNode node) throws ConfigurationException {

    }

}
