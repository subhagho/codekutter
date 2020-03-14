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

import com.codekutter.common.model.LockId;
import com.codekutter.common.utils.Monitoring;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Timer;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Extension of Reentrant Locks to support inter process locking.
 * Locks, like reentrant locks, are expected to be thread local and
 * will raise exceptions if shared between threads.
 */
@Getter
@Accessors(fluent = true)
public abstract class DistributedLock extends ReentrantLock implements Closeable {
    private static final long DEFAULT_LOCK_TIMEOUT = 15 * 60 * 1000; // 15 mins.
    /**
     * Timer to measure Lock latency.
     */
    protected Timer lockLatency = null;
    /**
     * Timer to measure unlock latency.
     */
    protected Timer unlockLatency = null;
    /**
     * Counter to measure total # of lock/Unlock errors.
     */
    protected Id callCounter = null;
    /**
     * Counter to measure total # of lock/Unlock errors.
     */
    protected Id errorCounter = null;
    /**
     * Unique ID for each lock instance.
     */
    private String instanceId;
    /**
     * Globally Unique lock ID (namespace + name)
     */
    private LockId id;
    /**
     * Lock instance owner thread ID
     */
    private long threadId;
    /**
     * Timeout for getting a lock.
     */
    private long lockGetTimeout = DEFAULT_LOCK_TIMEOUT;
    /**
     * Timeout to expire locks, if lock is held for
     * more than the timeout window it will be expired and
     * can be acquired by other processes.
     */
    private long lockExpiryTimeout = -1;
    private AbstractLockAllocator allocator;

    /**
     * Lock Constructor with namespace and name.
     *
     * @param namespace - Lock namespace.
     * @param name      - Lock name,
     */
    DistributedLock(@Nonnull String namespace,
                    @Nonnull String name,
                    @Nonnull AbstractLockAllocator allocator) {
        id = new LockId();
        id.setNamespace(namespace);
        id.setName(name);

        instanceId = UUID.randomUUID().toString();
        threadId = Thread.currentThread().getId();

        this.allocator = allocator;
    }

    /**
     * Constructor with Lock ID.
     *
     * @param id - Unique Lock ID
     */
    DistributedLock(@Nonnull LockId id, @Nonnull AbstractLockAllocator allocator) {
        this.id = id;

        instanceId = UUID.randomUUID().toString();
        threadId = Thread.currentThread().getId();

        this.allocator = allocator;
    }

    /**
     * Check if this lock instance was created by the calling thread.
     *
     * @throws LockException - If not owned by current thread.
     */
    void checkThread() throws LockException {
        if (threadId != Thread.currentThread().getId()) {
            throw new LockException(String.format("Lock not owned by current thread. [owner thread id=%d]", threadId));
        }
    }

    /**
     * Set the Lock acquire timeout.
     *
     * @param lockGetTimeout - Lock Acquire timeout.
     * @return - Self.
     */
    public DistributedLock withLockGetTimeout(long lockGetTimeout) {
        if (lockGetTimeout > 0)
            this.lockGetTimeout = lockGetTimeout;
        return this;
    }

    /**
     * Set the Lock expiry window.
     *
     * @param lockExpiryTimeout - Lock Expiry window.
     * @return - Self
     */
    public DistributedLock withLockExpiryTimeout(long lockExpiryTimeout) {
        if (lockExpiryTimeout > 0)
            this.lockExpiryTimeout = lockExpiryTimeout;
        return this;
    }

    @Override
    public void close() throws IOException {
        if (isLocked()) {
            unlock();
        }
        allocator.remove(this.id);
    }

    /**
     * Get this lock keys. (namespace + name)
     *
     * @return - Lock Key
     */
    @JsonIgnore
    public String getKey() {
        return String.format("%s::%s", id.getNamespace(), id.getName());
    }

    /**
     * Check if this lock instance is held by the current thread.
     *
     * @return - Held by current thread?
     */
    @Override
    public boolean isHeldByCurrentThread() {
        if (super.isHeldByCurrentThread()) {
            return (threadId == Thread.currentThread().getId());
        }
        return false;
    }

    protected void setupMetrics(String metricLockLatency,
                                String metricUnlockLatency,
                                String counterLocked,
                                String counterError) {
        lockLatency = Monitoring.addTimer(String.format(metricLockLatency, id().getNamespace(), id().getName()));
        unlockLatency = Monitoring.addTimer(String.format(metricUnlockLatency, id().getNamespace(), id().getName()));
        callCounter = Monitoring.addCounter(String.format(counterLocked, id().getNamespace(), id().getName()));
        errorCounter = Monitoring.addCounter(String.format(counterError, id().getNamespace(), id().getName()));
    }

    /**
     * Release this lock handle.
     *
     * @throws IOException
     */
    public abstract void remove() throws IOException;

    /**
     * Check the lock state is valid.
     *
     * @return - Is valid?
     */
    public abstract boolean isValid();
}
