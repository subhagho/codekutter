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
import com.google.common.base.Preconditions;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.annotation.Nonnull;
import javax.persistence.LockModeType;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DistributedDbLock extends DistributedLock {
    private Session session;
    private Transaction transaction = null;
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
        checkThread();
        super.lock();
        try {
            if (locked) return;
            checkTransaction();

            DbLockRecord record = session.find(DbLockRecord.class, id(), LockModeType.PESSIMISTIC_WRITE);
            if (record == null) {
                record = new DbLockRecord();
                record.setId(id());
                record.setInstanceId(instanceId());
                record.setLocked(true);
                record.setTimestamp(System.currentTimeMillis());

                session.save(record);
            }
            locked = true;
        } catch (Throwable t) {
            super.unlock();
        }
    }

    @Override
    public boolean tryLock() {
        Preconditions.checkState(session != null);
        checkThread();
        if (super.tryLock()) {
            try {
                if (locked) return true;
                checkTransaction();

                DbLockRecord record = session.find(DbLockRecord.class, id(), LockModeType.PESSIMISTIC_WRITE);
                if (record == null) {
                    record = new DbLockRecord();
                    record.setId(id());
                    record.setInstanceId(instanceId());
                    record.setLocked(true);
                    record.setTimestamp(System.currentTimeMillis());

                    session.save(record);
                }
                locked = true;

            } catch (Throwable t) {
                super.unlock();
            }
        }
        return false;
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        Preconditions.checkState(session != null);
        checkThread();
        if (super.tryLock(timeout, unit)) {
            try {
                if (locked) return true;
                checkTransaction();

                DbLockRecord record = session.find(DbLockRecord.class, id(), LockModeType.PESSIMISTIC_WRITE);
                if (record == null) {
                    record = new DbLockRecord();
                    record.setId(id());
                    record.setInstanceId(instanceId());
                    record.setLocked(true);
                    record.setTimestamp(System.currentTimeMillis());

                    session.save(record);
                }
                locked = true;

            } catch (Throwable t) {
                super.unlock();
            }
        }
        return false;
    }

    @Override
    public void unlock() {
        Preconditions.checkState(session != null);
        checkThread();
        if (locked) {
            if (transaction != null && transaction.isActive()) {
                transaction.commit();
            }
            transaction = null;
            super.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (session != null) {
            session.close();
        }
    }

    private void checkTransaction() {
        if (!session.isJoinedToTransaction()) {
            transaction = session.beginTransaction();
        }
    }
}
