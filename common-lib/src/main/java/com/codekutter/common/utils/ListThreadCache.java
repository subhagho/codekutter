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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class ListThreadCache<T> {
    private Map<Long, List<T>> cache = new HashMap<>();
    private ReentrantLock cacheLock = new ReentrantLock();

    public T put(@Nonnull T value) {
        long threadId = Thread.currentThread().getId();
        if (!cache.containsKey(threadId)) {
            cacheLock.lock();
            try {
                if (!cache.containsKey(threadId)) {
                    List<T> values = new ArrayList<>();
                    cache.put(threadId, values);
                }
            } finally {
                cacheLock.unlock();
            }
        }
        List<T> values = cache.get(threadId);
        values.add(value);
        return value;
    }

    public List<T> get() {
        long threadId = Thread.currentThread().getId();
        if (cache.containsKey(threadId)) {
            return new ArrayList<>(cache.get(threadId));
        }
        return null;
    }

    public T get(int index) {
        List<T> values = get();
        if (values != null && !values.isEmpty()) {
            return values.get(index);
        }
        return null;
    }

    public boolean remove(T value) {
        long threadId = Thread.currentThread().getId();
        List<T> values = cache.get(threadId);
        if (values != null && !values.isEmpty()) {
            return values.remove(value);
        }
        return false;
    }

    public boolean remove(int index) {
        long threadId = Thread.currentThread().getId();
        List<T> values = cache.get(threadId);
        if (values != null && !values.isEmpty()) {
            if (index < values.size()) {
                T value = values.remove(index);
                return value != null;
            }
        }
        return false;
    }

    public void clear() {
        List<T> values = get();
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
        List<T> values = get();
        if (values != null) {
            return values.size();
        }
        return 0;
    }

    public boolean containsThread() {
        return cache.containsKey(Thread.currentThread().getId());
    }
}
