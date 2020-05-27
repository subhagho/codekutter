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
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

@Getter
@Setter
@Accessors(fluent = true)
public class MultiMapCache<K extends IKey, T extends IKeyed<K>> extends AbstractMultiMapCache<K, T> {
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Multimap<K, T> cache = null;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Multimap<K, T> backupCache = null;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Multimap<K, T> cache01 = ArrayListMultimap.create();
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Multimap<K, T> cache02 = ArrayListMultimap.create();

    public MultiMapCache(@Nonnull Class<? extends T> entityType) {
        super(entityType);
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
        super.configure(node);
        try {
            load();

            state.setState(EObjectState.Available);
            loaderThread = new ManagedThread(this, String.format("%s::%s", getClass().getCanonicalName(), name));
            loaderThread.start();
        } catch (Exception ex) {
            state.setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public Collection<T> get(@Nonnull K key) {
        Preconditions.checkState(state.getState() == EObjectState.Available);
        if (cache != null && !cache.isEmpty()) return cache.get(key);
        return null;
    }

    @Override
    public Set<K> keySet() {
        Preconditions.checkState(state.getState() == EObjectState.Available);
        if (cache != null && !cache.isEmpty()) return cache.keySet();
        return null;
    }

    @Override
    public Collection<T> values() {
        Preconditions.checkState(state.getState() == EObjectState.Available);
        if (cache != null && !cache.isEmpty()) return cache.values();
        return null;
    }

    @Override
    public boolean isEmpty() {
        if (state.getState() == EObjectState.Available && cache != null) {
            return cache.isEmpty();
        }
        return true;
    }

    @Override
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
            Multimap<K, T> data = loader.read(null);
            if (data != null && !data.isEmpty()) {
                backupCache.putAll(data);
                Multimap<K, T> tcache = cache;
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
