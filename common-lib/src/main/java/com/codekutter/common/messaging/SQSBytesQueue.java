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
import com.codekutter.common.utils.LogUtils;

import javax.jms.JMSException;
import javax.jms.Message;
import java.security.Principal;

public class SQSBytesQueue extends AbstractSQSQueue<DefaultBytesMessage> {

    @Override
    public Message message(DefaultBytesMessage message) throws JMSException {
        try {
            return DefaultBytesMessageUtils.message(session(), queue(), message);
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }

    @Override
    public DefaultBytesMessage message(Message message, Principal user) throws JMSException {
        try {
            return DefaultBytesMessageUtils.message(message);
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new JMSException(ex.getLocalizedMessage());
        }
    }
}
