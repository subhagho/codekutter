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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadCache<T> implements Closeable {
    private Map<Long, T> cache = new HashMap<>();
    private ReentrantLock cacheLock = new ReentrantLock();

    public T put(T elem) {
        cacheLock.lock();
        try {
            long threadId = Thread.currentThread().getId();
            cache.put(threadId, elem);
            return cache.get(threadId);
        } finally {
            cacheLock.unlock();
        }
    }

    public T remove() {
        cacheLock.lock();
        try {
            long threadId = Thread.currentThread().getId();
            if (cache.containsKey(threadId)) {
                return cache.remove(threadId);
            }
            return null;
        } finally {
            cacheLock.unlock();
        }
    }

    public T get() {
        long threadId = Thread.currentThread().getId();
        return cache.get(threadId);
    }

    public boolean contains() {
        long threadId = Thread.currentThread().getId();
        return cache.containsKey(threadId);
    }

    @Override
    public void close() throws IOException {
        cacheLock.lock();
        try {
            if (!cache.isEmpty()) {
                for (long id : cache.keySet()) {
                    T value = cache.get(id);
                    if (value instanceof Closeable) {
                        ((Closeable) value).close();
                    }
                }
                cache.clear();
            }
        } finally {
            cacheLock.unlock();
        }
    }

    public void close(ICloseDelegate<T> delegate) throws IOException {
        cacheLock.lock();
        try {
            if (!cache.isEmpty()) {
                for (long id : cache.keySet()) {
                    T value = cache.get(id);
                    delegate.close(value);
                }
                cache.clear();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            cacheLock.unlock();
        }
    }
}
