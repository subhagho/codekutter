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

import com.codekutter.common.ICloseDelegate;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class MapThreadCache<K, V> implements Closeable {
    private final Map<Long, Map<K, V>> cache = new ConcurrentHashMap<>();

    public V put(@Nonnull K key, @Nonnull V value) {
        long threadId = Thread.currentThread().getId();
        cache.computeIfAbsent(threadId, k -> new ConcurrentHashMap<>()).put(key, value);
        return value;
    }

    public Map<K, V> get() {
        long threadId = Thread.currentThread().getId();
        return Collections.unmodifiableMap(cache.getOrDefault(threadId, Collections.emptyMap()));
    }

    public V get(K key) {
        return get().get(key);
    }

    public boolean containsKey(K key) {
        return get().containsKey(key);
    }

    public boolean remove(K key) {
        long threadId = Thread.currentThread().getId();

        Map<K, V> values = cache.getOrDefault(threadId, Collections.emptyMap());
        return values.remove(key) != null;
    }

    public void clear() {
        Map<K, V> values = get();
        if (values != null) {
            values.clear();
            cache.remove(Thread.currentThread().getId());
        }
    }

    public int size() {
        return get().size();
    }

    public void dispose() {
        cache.values().forEach(Map::clear);
        cache.clear();
    }

    public boolean containsThread() {
        return cache.containsKey(Thread.currentThread().getId());
    }

    @Override
    public void close() throws IOException {
        for (Map<K, V> eachMap : cache.values()) {
            for (V value : eachMap.values()) {
                if (value instanceof Closeable) {
                    ((Closeable) value).close();
                }
            }
        }
        cache.clear();
    }

    public void close(ICloseDelegate<V> delegate) throws IOException {
        for (Map<K, V> eachMap : cache.values()) {
            for (V value : eachMap.values()) {
                try {
                    delegate.close(value);
                } catch (Exception ex) {
                    throw new IOException(ex);
                }
            }
            eachMap.clear();
        }
    }
}
