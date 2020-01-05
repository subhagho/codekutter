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
import com.codekutter.common.utils.DateTimeUtils;
import com.codekutter.common.utils.LogUtils;
import com.google.common.base.Preconditions;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.annotation.Nonnull;
import javax.persistence.LockModeType;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DistributedDbLock extends DistributedLock {
    private static final long DEFAULT_SLEEP_INTERVAL = 300;

    private Session session;
    private boolean locked = false;

    public DistributedDbLock(@Nonnull String namespace, @Nonnull String name) {
        super(namespace, name);
    }

    public DistributedDbLock(@Nonnull LockId id) {
        super(id);
    }

    public DistributedDbLock withSession(@Nonnull Session session) {
        this.session = session;
        return this;
    }

    @Override
    public void lock() {
        Preconditions.checkState(session != null);
        if (!tryLock(lockGetTimeout(), TimeUnit.MILLISECONDS)) {
            throw new LockException(String.format("[%s][%s] Timeout getting lock.", id().getNamespace(), id().getName()));
        }
    }

    @Override
    public boolean tryLock() {
        Preconditions.checkState(session != null);
        checkThread();
        if (super.tryLock()) {
            if (locked) return true;
            Transaction tnx = session.beginTransaction();
            try {
                DbLockRecord record = checkExpiry(fetch(session, true));
                if (record.isLocked() && instanceId().compareTo(record.getInstanceId()) == 0) {
                    session.save(record);
                    tnx.commit();
                    locked = true;
                } else if (!record.isLocked()) {
                    record.setInstanceId(instanceId());
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
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        Preconditions.checkState(session != null);
        checkThread();
        try {
            long start = System.currentTimeMillis();
            long period = DateTimeUtils.period(timeout, unit);
            if (super.tryLock(timeout, unit)) {
                if (locked) return true;
                while (true) {
                    if ((System.currentTimeMillis() - start) > period) break;
                    Transaction tnx = session.beginTransaction();
                    try {
                        DbLockRecord record = checkExpiry(fetch(session, true));
                        if (record.isLocked() && instanceId().compareTo(record.getInstanceId()) == 0) {
                            session.save(record);
                            tnx.commit();
                            locked = true;
                            break;
                        } else if (!record.isLocked()) {
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
                }
            }
            return locked;
        } catch (Throwable t) {
            throw new LockException(t);
        }
    }

    @Override
    public void unlock() {
        Preconditions.checkState(session != null);
        checkThread();
        if (locked) {
            DbLockRecord record = fetch(session, false);
            if (record == null) {
                throw new LockException(String.format("[%s][%s][%s] Lock record not found.", id().getNamespace(), id().getName(), threadId()));
            }
            if (record.isLocked() && instanceId().compareTo(record.getInstanceId()) == 0) {
                Transaction tnx = session.beginTransaction();
                try {
                    record.setLocked(false);
                    record.setInstanceId(null);
                    record.setTimestamp(-1);

                    session.save(record);
                    tnx.commit();
                } catch (Throwable t) {
                    tnx.rollback();
                    throw new LockException(t);
                }
            } else {
                throw new LockException(String.format("[%s][%s] Lock not held by current thread. [thread=%d]", id().getNamespace(), id().getName(), threadId()));
            }
            locked = false;
        } else {
            throw new LockException(String.format("[%s][%s] Lock not held by current thread. [thread=%d]", id().getNamespace(), id().getName(), threadId()));
        }
    }

    @Override
    public void close() throws IOException {
        if (session != null) {
            session.close();
        }
    }

    @Override
    public boolean isLocked() {
        if (super.isLocked()) return true;
        else {
            DbLockRecord record = fetch(session, false);
            return record.isLocked();
        }
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
    private DbLockRecord fetch(Session session, boolean create) throws LockException {
        DbLockRecord record = session.find(DbLockRecord.class, id(), LockModeType.PESSIMISTIC_WRITE);
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
}
