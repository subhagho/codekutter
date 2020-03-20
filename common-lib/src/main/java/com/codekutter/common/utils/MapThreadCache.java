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

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class MapThreadCache<K, V> {
    private Map<Long, Map<K, V>> cache = new HashMap<>();
    private ReentrantLock cacheLock = new ReentrantLock();

    public V put(@Nonnull K key, @Nonnull V value) {
        long threadId = Thread.currentThread().getId();
        if (!cache.containsKey(threadId)) {
            cacheLock.lock();
            try {
                if (!cache.containsKey(threadId)) {
                    Map<K, V> values = new HashMap<>();
                    cache.put(threadId, values);
                }
            } finally {
                cacheLock.unlock();
            }
        }
        Map<K, V> values = cache.get(threadId);
        values.put(key, value);
        return value;
    }

    public Map<K, V> get() {
        long threadId = Thread.currentThread().getId();
        if (cache.containsKey(threadId)) {
            return new HashMap<>(cache.get(threadId));
        }
        return null;
    }

    public V get(K key) {
        Map<K, V> values = get();
        if (values != null && !values.isEmpty()) {
            return values.get(key);
        }
        return null;
    }

    public boolean containsKey(K key) {
        if (containsThread()) {
            return get().containsKey(key);
        }
        return false;
    }

    public boolean remove(K key) {
        long threadId = Thread.currentThread().getId();
        Map<K, V> values = cache.get(threadId);
        if (values != null && !values.isEmpty()) {
            if (values.containsKey(key)) {
                V value = values.remove(key);
                return value != null;
            }
            Preconditions.checkState(get(key) == null);
        }
        return false;
    }

    public void clear() {
        Map<K, V> values = get();
        if (values != null) {
            values.clear();
            cacheLock.lock();
            try {
                cache.remove(Thread.currentThread().getId());
            } finally {
                cacheLock.unlock();
            }
        }
    }

    public int size() {
        Map<K, V> values = get();
        if (values != null) return values.size();
        return 0;
    }

    public void dispose() {
        for (long id : cache.keySet()) {
            Map<K, V> map = cache.get(id);
            map.clear();
        }
        cache.clear();
    }

    public boolean containsThread() {
        return cache.containsKey(Thread.currentThread().getId());
    }
}
