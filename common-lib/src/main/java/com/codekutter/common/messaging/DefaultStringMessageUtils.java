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

import com.codekutter.common.GlobalConstants;
import com.codekutter.common.model.DefaultStringMessage;
import com.codekutter.common.utils.LogUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.security.Principal;

public class DefaultStringMessageUtils {

    public static Message message(@Nonnull Session session,
                               @Nonnull String queue,
                               @Nonnull DefaultStringMessage message) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(queue));
        message.setQueue(queue);
        message.setTimestamp(System.currentTimeMillis());

        ObjectMapper mapper = GlobalConstants.getJsonMapper();
        String json = mapper.writeValueAsString(message);
        return session.createTextMessage(json);
    }

    public static DefaultStringMessage message(@Nonnull Message message) throws Exception {
        if (!(message instanceof TextMessage)) {
            throw new JMSException(String.format("Message type not supported. [type=%s]", message.getClass().getCanonicalName()));
        }
        ObjectMapper mapper = GlobalConstants.getJsonMapper();
        DefaultStringMessage m = mapper.readValue(((TextMessage) message).getText(), DefaultStringMessage.class);
        m.setMessageId(message.getJMSMessageID());
        return m;
    }

    public static <M> M messageEntity(@Nonnull Message message, @Nonnull Class<? extends M> type) throws Exception {
        DefaultStringMessage m = message(message);
        String json = m.getBody();
        if (!Strings.isNullOrEmpty(json)) {
            M entity = GlobalConstants.getJsonMapper().readValue(json, type);
            LogUtils.debug(DefaultStringMessageUtils.class, entity);
            return entity;
        }
        return null;
    }

    public static byte[] getBytes(@Nonnull DefaultStringMessage message) throws Exception {
        String json = GlobalConstants.getJsonMapper().writeValueAsString(message);
        if (!Strings.isNullOrEmpty(json)) {
            return json.getBytes(StandardCharsets.UTF_8);
        }
        return new byte[0];
    }

    public static DefaultStringMessage readMessage(@Nonnull byte[] body) throws Exception {
        DefaultStringMessage message = GlobalConstants.getJsonMapper().readValue(body, DefaultStringMessage.class);
        LogUtils.debug(DefaultStringMessageUtils.class, message);
        return message;
    }

    public static <M> M readEntity(@Nonnull byte[] body, @Nonnull Class<? extends M> type) throws Exception {
        DefaultStringMessage message = readMessage(body);
        String json = message.getBody();
        if (!Strings.isNullOrEmpty(json)) {
            M entity = GlobalConstants.getJsonMapper().readValue(json, type);
            LogUtils.debug(DefaultStringMessageUtils.class, entity);
            return entity;
        }
        return null;
    }
}
