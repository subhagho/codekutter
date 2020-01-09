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
import com.google.common.base.Preconditions;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Timer;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Distributed Lock implementation that uses ZooKeeper inter-process lock backend to
 * persist and synchronize lock(s) and state(s).
 */
public class DistributedZkLock extends DistributedLock {
    private static final class Metrics {
        private static final String METRIC_LATENCY_LOCK = String.format("%s.%s.%s.LOCK", DistributedZkLock.class.getName(), "%s", "%s");
        private static final String METRIC_LATENCY_UNLOCK = String.format("%s.%s.%s.UNLOCK", DistributedZkLock.class.getName(), "%s", "%s");
        private static final String METRIC_COUNTER_ERROR = String.format("%s.%s.%s.ERRORS", DistributedZkLock.class.getName(), "%s", "%s");
        private static final String METRIC_COUNTER_CALLS = String.format("%s.%s.%s.CALLS", DistributedZkLock.class.getName(), "%s", "%s");
    }

    /**
     * Default Lock get timeout.
     */
    private static final int DEFAULT_LOCK_TIMEOUT = 500;

    /**
     * ZooKeeper Inter-process Mutex instance.
     */
    private InterProcessMutex mutex = null;

    public DistributedZkLock(@Nonnull String namespace, @Nonnull String name) {
        super(namespace, name);
        setupMetrics(Metrics.METRIC_LATENCY_LOCK,
                Metrics.METRIC_LATENCY_UNLOCK,
                Metrics.METRIC_COUNTER_CALLS,
                Metrics.METRIC_COUNTER_ERROR);
    }

    public DistributedZkLock(@Nonnull LockId id) {
        super(id);
        setupMetrics(Metrics.METRIC_LATENCY_LOCK,
                Metrics.METRIC_LATENCY_UNLOCK,
                Metrics.METRIC_COUNTER_CALLS,
                Metrics.METRIC_COUNTER_ERROR);
    }

    public DistributedZkLock withMutex(@Nonnull InterProcessMutex mutex) {
        this.mutex = mutex;
        return this;
    }

    @Override
    public void lock() {
        Preconditions.checkState(mutex != null);
        checkThread();
        Monitoring.increment(callCounter.name(), null);
        lockLatency.record(() -> {
            try {
                if (!mutex.isAcquiredInThisProcess())
                    if (!tryLock()) {
                        throw new LockException(String.format("[%s][%s] Timeout getting lock.", id().getNamespace(), id().getName()));
                    }
            } catch (Throwable ex) {
                Monitoring.increment(errorCounter.name(), null);
                throw new LockException(ex);
            }
        });
    }

    @Override
    public boolean tryLock() {
        Preconditions.checkState(mutex != null);
        checkThread();
        Monitoring.increment(callCounter.name(), null);
        try {
            return lockLatency.record(() -> {
                if (super.tryLock()) {
                    try {
                        if (mutex.isAcquiredInThisProcess()) {
                            return true;
                        }
                        return mutex.acquire(DEFAULT_LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
                    } catch (Throwable t) {
                        super.unlock();
                        Monitoring.increment(errorCounter.name(), null);
                        throw new LockException(t);
                    }
                }
                return false;
            });
        } catch (Exception ex) {
            throw new LockException(ex);
        }
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        Preconditions.checkState(mutex != null);
        checkThread();
        Monitoring.increment(callCounter.name(), null);
        try {
            return lockLatency.record(() -> {
                if (super.tryLock(timeout, unit)) {
                    try {
                        if (mutex.isAcquiredInThisProcess())
                            return true;
                        return mutex.acquire(timeout, unit);
                    } catch (Throwable t) {
                        super.unlock();
                        Monitoring.increment(errorCounter.name(), null);
                        throw new LockException(t);
                    }
                }
                return false;
            });
        } catch (Exception ex) {
            throw new LockException(ex);
        }
    }

    @Override
    public void unlock() {
        Preconditions.checkState(mutex != null);
        checkThread();
        unlockLatency.record(() -> {
            try {
                if (mutex.isAcquiredInThisProcess()) {
                    mutex.release();
                } else {
                    throw new LockException(String.format("[%s][%s] Lock not held by current thread. [thread=%d]", id().getNamespace(), id().getName(), threadId()));
                }
                super.unlock();
            } catch (Throwable t) {
                super.unlock();
                Monitoring.increment(errorCounter.name(), null);
                throw new LockException(t);
            }
        });
    }

    @Override
    public boolean isLocked() {
        Preconditions.checkState(mutex != null);
        if (super.isLocked()) {
            return mutex.isAcquiredInThisProcess();
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        // Do nothing...
    }
}
