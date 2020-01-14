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

package com.codekutter.common.locking;

import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.LockId;
import com.codekutter.common.model.ObjectState;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract base class to define Lock Allocators. Allocators create instances of
 * the specific types of distributed locks.
 *
 * @param <T> - Connection handle type for persisting the lock instance.
 */
@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "lock-allocator")
public abstract class AbstractLockAllocator<T> implements IConfigurable, Closeable {
    private static final long DEFAULT_LOCK_TIMEOUT = 60 * 60 * 1000; // 1 Hr.

    /**
     * Timeout for expiring locks - used to prevent lock starvation
     * due to lock instances that haven't been released.
     */
    @ConfigValue(name = "lockExpiryTimeout")
    private long lockExpiryTimeout = DEFAULT_LOCK_TIMEOUT;
    /**
     * Max Timeout to acquire a lock - Default timeout for getting a lock.
     */
    @ConfigValue(name = "lockGetTimeout")
    private long lockTimeout = -1;
    /**
     * Class type of the lock instance.
     */
    @ConfigAttribute(name = "lockType", required = true)
    private Class<? extends DistributedLock> lockType;
    /**
     * Connection instance used to persist the lock.
     */
    @Setter(AccessLevel.NONE)
    protected AbstractConnection<T> connection;
    /**
     * Map containing the thread instances of this lock.
     */
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private Map<Long, Map<LockId, DistributedLock>> threadLocks = new ConcurrentHashMap<>();
    /**
     * State of this lock instance.
     */
    @Setter(AccessLevel.NONE)
    private ObjectState state = new ObjectState();


    /**
     * Allocate a new instance of this lock. Locks are not thread safe hence
     * lock instance usage is per thread.
     *
     * @param namespace - Namespace of the Lock.
     * @param name      -Lock name.
     * @return - New or Thread Local Lock instance.
     * @throws LockException
     */
    public DistributedLock allocate(@Nonnull String namespace, @Nonnull String name) throws LockException {
        try {
            state.check(EObjectState.Available, getClass());
            LockId id = new LockId();
            id.setNamespace(namespace);
            id.setName(name);

            DistributedLock lock = checkThreadCache(id);
            if (lock == null) {
                lock = createInstance(id);

                Map<LockId, DistributedLock> locks = threadLocks.get(lock.threadId());
                if (locks == null) {
                    locks = new HashMap<>();
                    threadLocks.put(lock.threadId(), locks);
                }
                locks.put(lock.id(), lock);
            }
            return lock;
        } catch (Throwable t) {
            throw new LockException(t);
        }
    }

    /**
     * Release a specific instance of a lock.
     *
     * @param id - Unique Lock instance ID
     *
     * @return - Lock is released?
     * @throws LockException
     */
    public boolean release(@Nonnull LockId id) throws LockException {
        boolean ret = false;
        try {
            state.check(EObjectState.Available, getClass());
            DistributedLock lock = checkThreadCache(id);
            if (lock != null) {
                Map<LockId, DistributedLock> locks = threadLocks.get(lock.threadId());
                if (locks != null) {
                    long threadId = lock.threadId();
                    lock = locks.remove(id);
                    if (lock != null) {
                        if (lock.isLocked()) {
                            lock.unlock();
                        }
                        lock.close();
                        ret = true;
                    }
                    if (locks.isEmpty()) {
                        threadLocks.remove(threadId);
                    }
                }
            }
            return ret;
        } catch (Throwable t) {
            throw new LockException(t);
        }
    }

    private DistributedLock checkThreadCache(@Nonnull LockId id) {
        long threadId = Thread.currentThread().getId();
        if (threadLocks.containsKey(threadId)) {
            Map<LockId, DistributedLock> locks = threadLocks.get(threadId);
            if (locks.containsKey(id)) {
                return locks.get(id);
            }
        }
        return null;
    }

    /**
     * Close a thread local instance of a
     * Distributed Lock.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        state.setState(EObjectState.Disposed);
        if (!threadLocks.isEmpty()) {
            for (long tid : threadLocks.keySet()) {
                Map<LockId, DistributedLock> locks = threadLocks.get(tid);
                if (!locks.isEmpty()) {
                    for (LockId id : locks.keySet()) {
                        DistributedLock lock = locks.get(id);
                        lock.close();
                    }
                }
                locks.clear();
            }
            threadLocks.clear();
        }
    }

    /**
     * Create/Get a new instance of this type of Distributed Lock.
     *
     * @param id - Unique Lock ID to Create/Get instance.
     *
     * @return - Lock instance.
     * @throws LockException
     */
    protected abstract DistributedLock createInstance(@Nonnull LockId id) throws LockException;
}