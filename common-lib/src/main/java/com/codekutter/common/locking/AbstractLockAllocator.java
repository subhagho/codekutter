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

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "lock-allocator")
public abstract class AbstractLockAllocator<T> implements IConfigurable, Closeable {
    private static final long DEFAULT_LOCK_TIMEOUT = 60 * 60 * 1000; // 1 Hr.

    @ConfigValue(name = "lockExpiryTimeout")
    private long lockExpiryTimeout = DEFAULT_LOCK_TIMEOUT;
    @ConfigValue(name = "lockGetTimeout")
    private long lockTimeout = -1;
    @ConfigAttribute(name = "lockType", required = true)
    private Class<? extends DistributedLock> lockType;
    @Setter(AccessLevel.NONE)
    protected AbstractConnection<T> connection;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private Map<Long, Map<LockId, DistributedLock>> threadLocks = new ConcurrentHashMap<>();
    @Setter(AccessLevel.NONE)
    private ObjectState state = new ObjectState();


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

    public DistributedLock checkThreadCache(@Nonnull LockId id) {
        long threadId = Thread.currentThread().getId();
        if (threadLocks.containsKey(threadId)) {
            Map<LockId, DistributedLock> locks = threadLocks.get(threadId);
            if (locks.containsKey(id)) {
                return locks.get(id);
            }
        }
        return null;
    }

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

    protected abstract DistributedLock createInstance(@Nonnull LockId id) throws LockException;
}
