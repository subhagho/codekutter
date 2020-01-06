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
import com.codekutter.common.model.ObjectState;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ConfigPath(path = "distributed-locks")
public class DistributedLockFactory implements IConfigurable {
    private Map<Class<? extends DistributedLock>, AbstractLockAllocator<?>> allocators = new HashMap<>();
    private ObjectState state = new ObjectState();

    public DistributedLock getLock(@Nonnull Class<? extends DistributedLock> type,
                                   @Nonnull String namespace,
                                   @Nonnull String name) throws LockException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(namespace));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        try {
            state.check(EObjectState.Available, getClass());
            AbstractLockAllocator<?> allocator = allocators.get(type);
            if (allocator != null) {
                return allocator.allocate(namespace, name);
            }
            return null;
        } catch (Throwable t) {
            throw new LockException(t);
        }
    }

    public DistributedLock getDbLock(@Nonnull String namespace, @Nonnull String name) throws LockException {
        return getLock(DistributedDbLock.class, namespace, name);
    }

    public DistributedLock getZkLock(@Nonnull String namespace, @Nonnull String name) throws LockException {
        return getLock(DistributedZkLock.class, namespace, name);
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        try {
            LogUtils.info(getClass(), String.format("Initializing Distributed Locking. [config path=%s]", node.getAbsolutePath()));
            if (node instanceof ConfigPathNode) {
                createAllocator((ConfigPathNode) node);
            } else if (node instanceof ConfigListElementNode) {
                List<ConfigElementNode> nodes = ((ConfigListElementNode) node).getValues();
                if (nodes != null && !nodes.isEmpty()) {
                    for (ConfigElementNode n : nodes) {
                        createAllocator((ConfigPathNode) n);
                    }
                }
            }
            state.setState(EObjectState.Available);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            state.setError(t);
            throw new ConfigurationException(t);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void createAllocator(ConfigPathNode node) throws ConfigurationException {
        AbstractConfigNode cnode = ConfigUtils.getPathNode(AbstractLockAllocator.class, node);
        if (!(cnode instanceof ConfigPathNode)) {
            throw new ConfigurationException(String.format("Invalid Allocator Factory configuration. [node=%s]", node.getAbsolutePath()));
        }
        String cname = ConfigUtils.getClassAttribute(cnode);
        if (Strings.isNullOrEmpty(cname)) {
            throw new ConfigurationException(String.format("Class type attribute missing. [node=%s]", cnode.getAbsolutePath()));
        }
        try {
            Class<? extends AbstractLockAllocator> cls = (Class<? extends AbstractLockAllocator>) Class.forName(cname);
            AbstractLockAllocator<?> allocator = cls.newInstance();
            allocator.configure(cnode);

            allocators.put(allocator.lockType(), allocator);
        } catch (Throwable t) {
            throw new ConfigurationException(t);
        }
    }

    private void dispose() {
        if (state.getState() == EObjectState.Available) {
            state.setState(EObjectState.Disposed);
        }
        for(AbstractLockAllocator<?> allocator : allocators.values()) {
            try {
                allocator.close();
            } catch (Exception ex) {
                LogUtils.error(getClass(), ex);
            }
        }
    }

    private final static DistributedLockFactory factory = new DistributedLockFactory();

    public static void setup(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        factory.configure(node);
    }

    public static void close() {
        factory.dispose();
    }

    public static DistributedLockFactory get() {
        return factory;
    }
}
