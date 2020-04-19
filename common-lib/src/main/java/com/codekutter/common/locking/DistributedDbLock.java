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

import com.codekutter.common.model.DbLockRecord;
import com.codekutter.common.model.LockId;
import com.codekutter.common.stores.ConnectionException;
import com.codekutter.common.stores.impl.HibernateConnection;
import com.codekutter.common.utils.DateTimeUtils;
import com.codekutter.common.utils.KeyValuePair;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.Monitoring;
import com.google.common.base.Preconditions;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.annotation.Nonnull;
import javax.persistence.LockModeType;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Distributed Lock implementation that uses a Data base backend to
 * persist and synchronize lock(s) and state(s).
 */
public class DistributedDbLock extends DistributedLock {
    /**
     * Default Sleep interval between lock queries to check for DB lock.
     */
    private static final long DEFAULT_SLEEP_INTERVAL = 300;
    /**
     * Hibernate DB session.
     */
    private HibernateConnection connection;
    /**
     * Session instance for this connection.
     */
    private Session session;
    /**
     * Local variable to indicate if this instance has
     * already acquired the lock.
     */
    private boolean locked = false;

    /**
     * Create Lock instance with the specified namespace/name.
     *
     * @param namespace - Lock namespace.
     * @param name      - Lock name
     */
    public DistributedDbLock(@Nonnull String namespace,
                             @Nonnull String name,
                             @Nonnull AbstractLockAllocator allocator) {
        super(namespace, name, allocator);
        setupMetrics(Metrics.METRIC_LATENCY_LOCK,
                Metrics.METRIC_LATENCY_UNLOCK,
                Metrics.METRIC_COUNTER_CALLS,
                Metrics.METRIC_COUNTER_ERROR);
    }

    /**
     * Create lock instance with the specified lock ID.
     *
     * @param id - Lock Instance ID
     */
    public DistributedDbLock(@Nonnull LockId id, @Nonnull AbstractLockAllocator allocator) {
        super(id, allocator);
        setupMetrics(Metrics.METRIC_LATENCY_LOCK,
                Metrics.METRIC_LATENCY_UNLOCK,
                Metrics.METRIC_COUNTER_CALLS,
                Metrics.METRIC_COUNTER_ERROR);
    }

    /**
     * Set the DB session to be used to persist this lock instance.
     *
     * @param connection - Hibernate DB connection.
     * @return - Self
     */
    public DistributedDbLock withConnection(@Nonnull HibernateConnection connection) throws ConnectionException {
        this.connection = connection;
        this.session = connection.connection();
        return this;
    }

    /**
     * Acquire the lock. Use the default lock timeout.
     * Will throw exception if lock not acquired within the
     * default timeout window.
     */
    @Override
    public void lock() {
        Preconditions.checkState(connection != null);
        if (!tryLock(lockGetTimeout(), TimeUnit.MILLISECONDS)) {
            Monitoring.increment(errorCounter.name(), (KeyValuePair<String, String>[]) null);
            throw new LockException(String.format("[%s][%s] Timeout getting lock.", id().getNamespace(), id().getName()));
        }
    }

    /**
     * Try to acquire the lock, will return false if
     * lock acquire failed.
     *
     * @return - Locked?
     */
    @Override
    public boolean tryLock() {
        Preconditions.checkState(connection != null);
        checkThread();
        Monitoring.increment(callCounter.name(), (KeyValuePair<String, String>[]) null);
        try {
            return lockLatency.record(() -> {
                if (super.tryLock()) {
                    if (locked) return true;
                    Transaction tnx = session.beginTransaction();
                    try {
                        DbLockRecord record = checkExpiry(fetch(session, true, true));
                        if (record.isLocked() && instanceId().compareTo(record.getInstanceId()) == 0) {
                            session.save(record);
                            tnx.commit();
                            locked = true;
                        } else if (!record.isLocked()) {
                            record.setInstanceId(instanceId());
                            record.setLocked(true);
                            record.setTimestamp(System.currentTimeMillis());
                            session.update(record);
                            tnx.commit();
                            locked = true;
                        } else
                            tnx.rollback();
                    } catch (Exception ex) {
                        tnx.rollback();
                        throw ex;
                    }
                }
                return locked;
            });
        } catch (Exception ex) {
            Monitoring.increment(errorCounter.name(), (KeyValuePair<String, String>[]) null);
            throw new LockException(ex);
        }
    }

    /**
     * Try to acquire the lock within the specified
     * timeout window. Will return false if lock not acquired.
     *
     * @param timeout - Timeout value
     * @param unit    - Time Unit for timeout.
     * @return - Locked?
     */
    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        Preconditions.checkState(connection != null);
        checkThread();
        Monitoring.increment(callCounter.name(), (KeyValuePair<String, String>[]) null);
        try {
            return lockLatency.record(() -> {
                long start = System.currentTimeMillis();
                long period = DateTimeUtils.period(timeout, unit);
                if (super.tryLock(timeout, unit)) {
                    if (locked) return true;
                    while (true) {
                        if ((System.currentTimeMillis() - start) > period) break;
                        Transaction tnx = session.beginTransaction();
                        try {
                            DbLockRecord record = checkExpiry(fetch(session, true, true));
                            if (record.isLocked() && instanceId().compareTo(record.getInstanceId()) == 0) {
                                session.save(record);
                                tnx.commit();
                                locked = true;
                                break;
                            } else if (!record.isLocked()) {
                                record.setLocked(true);
                                record.setInstanceId(instanceId());
                                record.setTimestamp(System.currentTimeMillis());
                                session.update(record);
                                tnx.commit();
                                locked = true;
                                break;
                            }
                            tnx.rollback();
                        } catch (Exception ex) {
                            tnx.rollback();
                            throw ex;
                        }
                        Thread.sleep(DEFAULT_SLEEP_INTERVAL);
                    }
                }
                return locked;
            });
        } catch (Throwable t) {
            Monitoring.increment(errorCounter.name(), (KeyValuePair<String, String>[]) null);
            throw new LockException(t);
        }
    }

    /**
     * Release the lock. Will throw exception if
     * lock not acquired by current caller thread.
     */
    @Override
    public void unlock() {
        Preconditions.checkState(connection != null);
        checkThread();
        unlockLatency.record(() -> {
            if (locked) {
                try {
                    Transaction tnx = session.beginTransaction();
                    try {
                        DbLockRecord record = fetch(session, false, true);
                        if (record == null) {
                            throw new LockException(String.format("[%s][%s][%s] Lock record not found.", id().getNamespace(), id().getName(), threadId()));
                        }
                        if (record.isLocked() && instanceId().compareTo(record.getInstanceId()) == 0) {
                            record.setLocked(false);
                            record.setInstanceId(null);
                            record.setTimestamp(-1);

                            session.save(record);
                        } else {
                            throw new LockException(String.format("[%s][%s] Lock not held by current thread. [thread=%d]", id().getNamespace(), id().getName(), threadId()));
                        }
                        locked = false;
                        tnx.commit();
                    } catch (Throwable t) {
                        tnx.rollback();
                        throw new LockException(t);
                    } finally {
                        super.unlock();
                    }
                } catch (Exception ex) {
                    throw new LockException(ex);
                }
            } else {
                throw new LockException(String.format("[%s][%s] Lock not held by current thread. [thread=%d]", id().getNamespace(), id().getName(), threadId()));
            }
        });
    }

    /**
     * Check if this lock has been acquired, either by the
     * caller thread or some other process.
     *
     * @return - Is locked?
     */
    @Override
    public boolean isLocked() {
        Preconditions.checkState(connection != null);
        try {
            if (super.isLocked()) {
                DbLockRecord record = fetch(session, false, false);
                return record.isLocked();
            }
            return false;
        } catch (Exception ex) {
            throw new LockException(ex);
        }
    }

    /**
     * Release this lock handle.
     *
     * @throws IOException
     */
    @Override
    public void remove() throws IOException {
        try {
            if (connection != null) {
                connection.close(session);
            }
            connection = null;
            session = null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Check the lock state is valid.
     *
     * @return - Is valid?
     */
    @Override
    public boolean isValid() {
        return (session != null && session.isOpen());
    }

    private DbLockRecord checkExpiry(DbLockRecord record) {
        if (lockExpiryTimeout() > 0) {
            if (record.isLocked()) {
                long delta = System.currentTimeMillis() - record.getTimestamp();
                if (delta > lockExpiryTimeout()) {
                    LogUtils.error(getClass(),
                            String.format("[%s][%s] Lock expired, resetting lock. [instance id=%s][locked timestamp=%s]",
                                    id().getNamespace(), id().getName(), record.getInstanceId(), DateTimeUtils.toString(record.getTimestamp())));
                    record.setLocked(false);
                    record.setTimestamp(-1);
                    record.setInstanceId(null);
                }
            }
        }
        return record;
    }

    private DbLockRecord fetch(Session session, boolean create, boolean lock) throws LockException {
        LockModeType lt = LockModeType.NONE;
        if (lock) lt = LockModeType.PESSIMISTIC_WRITE;
        DbLockRecord record = session.find(DbLockRecord.class, id(), lt);
        if (record == null && create) {
            record = new DbLockRecord();
            record.setId(id());
            record.setInstanceId(instanceId());
            record.setLocked(true);
            record.setTimestamp(System.currentTimeMillis());

            session.save(record);
        }
        return record;
    }

    private static final class Metrics {
        private static final String METRIC_LATENCY_LOCK = String.format("%s.%s.%s.LOCK", DistributedDbLock.class.getName(), "%s", "%s");
        private static final String METRIC_LATENCY_UNLOCK = String.format("%s.%s.%s.UNLOCK", DistributedDbLock.class.getName(), "%s", "%s");
        private static final String METRIC_COUNTER_ERROR = String.format("%s.%s.%s.ERRORS", DistributedDbLock.class.getName(), "%s", "%s");
        private static final String METRIC_COUNTER_CALLS = String.format("%s.%s.%s.CALLS", DistributedDbLock.class.getName(), "%s", "%s");
    }
}
