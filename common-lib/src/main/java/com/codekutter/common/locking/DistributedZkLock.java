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

import com.google.common.base.Preconditions;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DistributedZkLock extends DistributedLock {
    private static final int DEFAULT_LOCK_TIMEOUT = 500;

    private InterProcessSemaphoreMutex mutex = null;

    public DistributedZkLock(@Nonnull String namespace, @Nonnull String name) {
        super(namespace, name);
    }

    public DistributedZkLock withMutex(@Nonnull InterProcessSemaphoreMutex mutex) {
        this.mutex = mutex;
        return this;
    }

    @Override
    public void lock() {
        Preconditions.checkState(mutex != null);
        checkThread();
        super.lock();
        try {
            if (!mutex.isAcquiredInThisProcess())
                mutex.acquire();
        } catch (Throwable t) {
            super.unlock();
        }
    }

    @Override
    public boolean tryLock() {
        Preconditions.checkState(mutex != null);
        checkThread();
        if (super.tryLock()) {
            try {
                if (!mutex.isAcquiredInThisProcess())
                    return mutex.acquire(DEFAULT_LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                super.unlock();
            }
        }
        return false;
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        Preconditions.checkState(mutex != null);
        checkThread();
        if (super.tryLock(timeout, unit)) {
            try {
                if (!mutex.isAcquiredInThisProcess())
                    return mutex.acquire(timeout, unit);
            } catch (Throwable t) {
                super.unlock();
            }
        }
        return false;
    }

    @Override
    public void unlock() {
        Preconditions.checkState(mutex != null);
        checkThread();
        super.unlock();
    }

    @Override
    public boolean isLocked() {
        Preconditions.checkState(mutex != null);
        return super.isLocked();
    }

    @Override
    public void close() throws IOException {
        // Do nothing...
    }
}
