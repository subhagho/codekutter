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

package com.codekutter.common.messaging;

import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.IKeyed;
import com.codekutter.common.utils.LogUtils;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CachedQueueSender<C, M extends IKeyed> implements Runnable {
    private static final long DEFAULT_INTERVAL = 15000;

    private final CachedQueue<C, M> queue;
    private final int partitionCount;
    private final Class<? extends M> type;
    private long startDelay = DEFAULT_INTERVAL;
    private long sendInterval = DEFAULT_INTERVAL;
    private final ExecutorService executorService;

    private Runnable[] runTasks;

    public CachedQueueSender(@Nonnull CachedQueue<C, M> queue, int partitionCount, @Nonnull Class<? extends M> type) {
        this.queue = queue;
        this.partitionCount = partitionCount;
        this.type = type;

        runTasks = new Runnable[partitionCount];
        for (int ii = 0; ii < partitionCount; ii++) {
            runTasks[ii] = new SendTask<C, M>(queue, ii, type);
        }
        executorService = Executors.newFixedThreadPool(partitionCount);
    }

    public CachedQueueSender<C, M> withStartDelay(long startDelay) {
        Preconditions.checkArgument(startDelay > 0);
        this.startDelay = startDelay;
        return this;
    }

    public CachedQueueSender<C, M> withSendInterval(long sendInterval) {
        Preconditions.checkArgument(sendInterval > 0);
        this.sendInterval = sendInterval;
        return this;
    }

    @Override
    public void run() {
        Preconditions.checkState(sendInterval > 0);
        long sleepInterval = startDelay;
        while (queue.state().getState() == EObjectState.Available) {
            try {
                Thread.sleep(sleepInterval);
                for (int ii = 0; ii < partitionCount; ii++) {
                    executorService.execute(runTasks[ii]);
                }
                sleepInterval = sendInterval;
            } catch (InterruptedException ex) {
                LogUtils.debug(getClass(), String.format("Thread interrupted [thread id=%d]", Thread.currentThread().getId()));
                // Do nothing.
            } catch (Exception ex) {
                LogUtils.error(getClass(), ex);
            }
        }
        executorService.shutdown();
    }

    private static class SendTask<C, M extends IKeyed> implements Runnable {
        private final int partitionId;
        private final CachedQueue<C, M> queue;
        private final String instanceId;
        private final Class<? extends M> type;

        public SendTask(@Nonnull CachedQueue<C, M> queue, int partitionId, @Nonnull Class<? extends M> type) {
            Preconditions.checkArgument(partitionId >= 0);
            this.partitionId = partitionId;
            this.queue = queue;
            this.type = type;

            instanceId = UUID.randomUUID().toString();
        }

        @Override
        public void run() {
            try {
                List<CachedQueue.MessageStruct<M>> messages = queue.sendNextBatch(instanceId, partitionId, type);
                if (messages != null)
                    LogUtils.info(getClass(), String.format("Processed message count = %d. [type=%s][queue=%s][partition=%d]",
                            messages.size(), type.getCanonicalName(), queue.name(), partitionId));
            } catch (Throwable t) {
                LogUtils.error(getClass(), t);
            }
        }
    }
}
