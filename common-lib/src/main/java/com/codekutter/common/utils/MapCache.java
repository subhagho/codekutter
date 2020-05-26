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

package com.codekutter.common.utils;

import com.codekutter.common.TimeWindow;
import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.IKey;
import com.codekutter.common.model.IKeyed;
import com.codekutter.common.model.ObjectState;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.transformers.TimeWindowParser;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "map-cache")
public class MapCache<K extends IKey, T extends IKeyed<K>> extends Runner implements IConfigurable, Closeable {
    @ConfigAttribute(required = true)
    private String name;
    @Setter(AccessLevel.NONE)
    private final Class<? extends T> entityType;
    @ConfigValue(name = "loader", required = true)
    private String loaderClass;
    @Setter(AccessLevel.NONE)
    private MapCacheLoader<K, T> loader;
    @ConfigValue(required = true, parser = TimeWindowParser.class)
    private TimeWindow refreshInterval;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<K, T> cache = null;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<K, T> backupCache = null;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<K, T> cache01 = new HashMap<>();
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<K, T> cache02 = new HashMap<>();
    @Setter(AccessLevel.NONE)
    private ObjectState state = new ObjectState();
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ManagedThread loaderThread;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private final ReentrantLock lock = new ReentrantLock();
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private long lastRunTime;

    public MapCache(@Nonnull Class<? extends T> entityType) {
        this.entityType = entityType;
        cache = cache01;
        backupCache = cache02;
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
            Class<? extends MapCacheLoader> cls = (Class<? extends MapCacheLoader>) Class.forName(loaderClass);
            loader = TypeUtils.createInstance(cls);
            AbstractConfigNode nn = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (nn == null) {
                throw new ConfigurationException(String.format("Type node not found. [type=%s][path=%s]", getClass().getCanonicalName(), node.getAbsolutePath()));
            }
            loader.configure(nn);
            load();

            state.setState(EObjectState.Available);
            loaderThread = new ManagedThread(this, String.format("%s::%s", getClass().getCanonicalName(), name));
            loaderThread.start();
        } catch (Exception ex) {
            state.setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    public T get(@Nonnull K key) {
        Preconditions.checkState(state.getState() == EObjectState.Available);
        if (cache != null && !cache.isEmpty()) return cache.get(key);
        return null;
    }

    public Set<K> keySet() {
        Preconditions.checkState(state.getState() == EObjectState.Available);
        if (cache != null && !cache.isEmpty()) return cache.keySet();
        return null;
    }

    public Collection<T> values() {
        Preconditions.checkState(state.getState() == EObjectState.Available);
        if (cache != null && !cache.isEmpty()) return cache.values();
        return null;
    }

    public boolean isEmpty() {
        if (state.getState() == EObjectState.Available && cache != null) {
            return cache.isEmpty();
        }
        return true;
    }

    public int size() {
        if (state.getState() == EObjectState.Available && cache != null) {
            return cache.size();
        }
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (!state.hasError()) {
            state.setState(EObjectState.Disposed);
        }
        try {
            cache01.clear();
            cache02.clear();
            loader.close();
            loaderThread.join();
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new IOException(ex);
        }
    }

    @Override
    public void doRun() throws Exception {
        try {
            while (state.getState() == EObjectState.Available) {
                long delta = (System.currentTimeMillis() - lastRunTime);
                if (delta < refreshInterval.period()) {
                    Thread.sleep(refreshInterval.period() - delta);
                }
                lastRunTime = System.currentTimeMillis();
                if (loader.needsReload()) {
                    load();
                }
            }
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw ex;
        }
    }

    private void load() throws CacheException {
        lock.lock();
        try {
            backupCache.clear();
            Collection<T> data = loader.read(null);
            if (data != null && !data.isEmpty()) {
                for (T record : data) {
                    backupCache.put(record.getKey(), record);
                }
                Map<K, T> tcache = cache;
                cache = backupCache;
                backupCache = tcache;
                LogUtils.info(getClass(), String.format("Refreshed cache [name=%s]. [#records=%d]", name, data.size()));
            } else {
                LogUtils.warn(getClass(), String.format("No data loaded for cache. [name=%s]", name));
            }
        } finally {
            lock.unlock();
        }
    }
}
