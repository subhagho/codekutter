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

import com.codekutter.common.model.DefaultBytesMessage;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class DefaultBytesMessageUtils {

    public static Message message(@Nonnull Session session,
                               @Nonnull String queue,
                               @Nonnull DefaultBytesMessage message) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(queue));
        message.setQueue(queue);
        message.setTimestamp(System.currentTimeMillis());

        BytesMessage m = session.createBytesMessage();
        m.setObjectProperty(DefaultBytesMessage.class.getName(), message);
        return m;
    }

    public static DefaultBytesMessage message(@Nonnull Message message) throws Exception {
        if (!(message instanceof BytesMessage)) {
            throw new JMSException(String.format("Message type not supported. [type=%s]", message.getClass().getCanonicalName()));
        }
        return (DefaultBytesMessage) message.getObjectProperty(DefaultBytesMessage.class.getName());
    }

    public static <M> M messageEntity(@Nonnull Message message, @Nonnull Class<? extends M> type) throws Exception {
        DefaultBytesMessage m = message(message);
        if (m.getBody() != null) {
            ByteArrayInputStream in = new ByteArrayInputStream(m.getBody());
            ObjectInputStream is = new ObjectInputStream(in);
            return  (M) is.readObject();
        }
        return null;
    }

    public static byte[] getBytes(@Nonnull DefaultBytesMessage message) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(message);
        oos.flush();
        return bos.toByteArray();
    }
}
