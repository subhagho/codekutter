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

import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.jms.JMSException;
import java.io.Closeable;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "queue")
public abstract class AbstractQueue<C, M> implements IConfigurable, Closeable {
    private static final long DEFAULT_RECEIVE_TIMEOUT = 30 * 1000;

    @ConfigAttribute(required = true)
    private String name;
    @ConfigValue(name = "receiveTimeout")
    private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;
    @Setter(AccessLevel.NONE)
    private AbstractConnection<C> connection;

    public abstract void send(@Nonnull M message) throws JMSException;

    public abstract M receive(long timeout) throws JMSException;

    public abstract boolean ack(@Nonnull String messageId) throws JMSException;

    public abstract List<M> receiveBatch(int maxResults, long timeout) throws JMSException;

    public List<M> receiveBatch(int maxResults) throws JMSException {
        return receiveBatch(maxResults, receiveTimeout);
    }

    public M receive() throws JMSException {
        return receive(receiveTimeout);
    }
}
