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
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "lock-allocator")
public abstract class AbstractLockAllocator<T> implements IConfigurable {
    private static final long DEFAULT_LOCK_TIMEOUT = 60 * 60 * 1000; // 1 Hr.

    private long lockTimeout = DEFAULT_LOCK_TIMEOUT;
    @ConfigAttribute(name = "type")
    private Class<? extends DistributedLock> lockType;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private Map<LockId, DistributedLock> locks = new ConcurrentHashMap<>();
    @Setter(AccessLevel.NONE)
    protected AbstractConnection<T> connection;

    public DistributedLock allocate(@Nonnull String namespace, @Nonnull String name) {
        DistributedLock lock = null;
        return null;
    }

    protected abstract DistributedLock createInstance() throws LockException;
}
