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
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.stores.impl.HibernateConnection;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.Session;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Setter
public class DbLockAllocator extends AbstractLockAllocator<Session> {
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<Long, Session> threadSessions = new HashMap<>();
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ReentrantLock lock = new ReentrantLock();

    @Override
    protected DistributedLock createInstance(@Nonnull LockId id) throws LockException {
        lock.lock();
        try {
            Session session = null;
            long threadId = Thread.currentThread().getId();
            if (threadSessions.containsKey(threadId)) {
                session = threadSessions.get(threadId);
                if (!session.isOpen()) {
                    session = null;
                }
            }
            if (session == null) {
                session = connection.connection();
                if (session == null) {
                    throw new LockException("Error getting DB session.");
                }
                threadSessions.put(threadId, session);
            }
            return new DistributedDbLock(id).withSession(session).
                    withLockExpiryTimeout(lockExpiryTimeout()).withLockGetTimeout(lockTimeout());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (cnode == null) {
                throw new ConfigurationException(String.format("Invalid DB Lock Allocator Configuration. [node=%s]", node.getAbsolutePath()));
            }
            LogUtils.info(getClass(), String.format("Initializing DataBase Locking. [config path=%s]", cnode.getAbsolutePath()));
            AbstractConfigNode ccnode = ConfigUtils.getPathNode(AbstractConnection.class, (ConfigPathNode) cnode);
            if (!(ccnode instanceof ConfigPathNode)) {
                throw new ConfigurationException(String.format("Connection configuration not found. [node=%s]", cnode.getAbsolutePath()));
            }
            connection = ConnectionManager.get().readConnection((ConfigPathNode) ccnode);

            state().setState(EObjectState.Available);
            LogUtils.debug(getClass(), String.format("Lock allocator initialized. [type=%s]", getClass().getCanonicalName()));
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            state().setError(t);
            throw new ConfigurationException(t);
        }
    }
}
