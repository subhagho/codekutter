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
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.security.Principal;

public class SQSJsonDbCachedQueue extends AbstractSQSDbCachedQueue<DefaultStringMessage> {
    @Override
    public TextMessage message(DefaultStringMessage message) throws JMSException {
        try {
            message.setQueue(queue());
            message.setTimestamp(System.currentTimeMillis());

            ObjectMapper mapper = GlobalConstants.getJsonMapper();
            String json = mapper.writeValueAsString(message);
            return session().createTextMessage(json);
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }

    @Override
    public DefaultStringMessage message(TextMessage message, Principal user) throws JMSException {
        try {
            ObjectMapper mapper = GlobalConstants.getJsonMapper();
            DefaultStringMessage m = mapper.readValue(message.getText(), DefaultStringMessage.class);
            m.setMessageId(message.getJMSMessageID());
            return m;
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }

    @Override
    public byte[] getBytes(@Nonnull DefaultStringMessage message) throws JMSException {
        try {
            String json = GlobalConstants.getJsonMapper().writeValueAsString(message);
            if (!Strings.isNullOrEmpty(json)) {
                return json.getBytes(StandardCharsets.UTF_8);
            }
            return new byte[0];
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }

    @Override
    public DefaultStringMessage readMessage(@Nonnull byte[] body) throws JMSException {
        try {
            DefaultStringMessage message = GlobalConstants.getJsonMapper().readValue(body, DefaultStringMessage.class);
            LogUtils.debug(getClass(), message);
            return message;
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }
}
