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
import com.codekutter.common.model.ObjectState;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.jms.JMSException;
import java.io.IOException;
import java.security.Principal;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public abstract class CachedQueue<C, M extends IKeyed> extends AbstractQueue<C, M> {
    public static final int DEFAULT_THREAD_POOL_SIZE = 8;
    public static final int DEFAULT_FETCH_BATCH_SIZE = 32;
    public static final int DEFAULT_RETRY_COUNT = 5;
    public static final long DEFAULT_START_DELAY = 20000;
    public static final long DEFAULT_SEND_DELAY = 10000;

    @ConfigValue
    protected int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    @ConfigValue
    protected int fetchBatchSize = DEFAULT_FETCH_BATCH_SIZE;
    @ConfigValue
    protected int retryCount = DEFAULT_RETRY_COUNT;
    @ConfigValue
    protected long startDelay = DEFAULT_START_DELAY;
    @ConfigValue
    protected long sendDelay = DEFAULT_SEND_DELAY;
    @ConfigAttribute(required = true)
    protected Class<? extends M> entityType;

    @Setter(AccessLevel.NONE)
    protected ObjectState state = new ObjectState();
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private CachedQueueSender<C, M> sender;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Thread senderThread;

    public ObjectState state() {
        return this.state;
    }

    public void start() {
        sender = new CachedQueueSender<>(this, threadPoolSize, entityType)
                .withStartDelay(startDelay).withSendInterval(sendDelay);
        senderThread = new Thread(sender);
    }

    @Override
    public void close() throws IOException {
        try {
            if (state.getState() == EObjectState.Available) {
                state.setState(EObjectState.Disposed);
            }
            senderThread.join();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public abstract List<MessageStruct<M>> sendNextBatch(@Nonnull String instanceId,
                                                         int partition,
                                                         @Nonnull Class<? extends M> type) throws JMSException;

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class MessageStruct<M> {
        private M message;
        private Principal user;
    }
}
