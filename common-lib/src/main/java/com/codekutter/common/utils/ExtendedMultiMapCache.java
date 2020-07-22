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

import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.IKey;
import com.codekutter.common.model.IKeyed;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.openhft.chronicle.map.ChronicleMap;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

@Getter
@Setter
@Accessors(fluent = true)
public class ExtendedMultiMapCache<K extends IKey, T extends IKeyed<K>> extends AbstractMapCache<K, T> {
    public static final int DEFAULT_CACHE_SIZE = 64000;
    public static final int DEFAULT_AVG_KEY_SIZE = 256;

    @Setter(AccessLevel.NONE)
    private final Class<? extends K> keyType;
    @ConfigValue
    private int maxCacheSize = DEFAULT_CACHE_SIZE;
    @ConfigValue
    private int averageKeySize = DEFAULT_AVG_KEY_SIZE;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ChronicleMap<K, T> cache = null;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ChronicleMap<K, T> backupCache = null;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ChronicleMap<K, T> cache01 = null;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ChronicleMap<K, T> cache02 = null;

    public ExtendedMultiMapCache(@Nonnull Class<? extends K> ketType, @Nonnull Class<? extends T> entityType) {
        super((entityType));
        this.keyType = ketType;
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
        super.configure(node);
        try {
            String mn = String.format("%s-%s", getClass().getName(), name);
            cache01 = (ChronicleMap<K, T>) ChronicleMap.of(keyType, entityType)
                    .name(mn + "-cache01").averageKeySize(averageKeySize).entries(maxCacheSize).create();
            cache02 = (ChronicleMap<K, T>) ChronicleMap.of(keyType, entityType)
                    .name(mn + "-cache02").averageKeySize(averageKeySize).entries(maxCacheSize).create();
            cache = cache01;
            backupCache = cache02;

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
            if (cache01 != null) {
                cache01.clear();
                cache01.close();
            }
            if (cache02 != null) {
                cache02.clear();
                cache02.close();
            }
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
                ChronicleMap<K, T> tcache = cache;
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
