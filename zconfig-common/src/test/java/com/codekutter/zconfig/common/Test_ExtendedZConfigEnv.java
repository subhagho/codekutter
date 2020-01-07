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

package com.codekutter.zconfig.common;

import com.amazon.sqs.javamessaging.SQSConnection;
import com.codekutter.common.locking.DistributedLock;
import com.codekutter.common.locking.DistributedLockFactory;
import com.codekutter.common.messaging.AbstractQueue;
import com.codekutter.common.messaging.QueueManager;
import com.codekutter.common.model.DefaultStringMessage;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.model.Version;
import com.google.common.base.Strings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class Test_ExtendedZConfigEnv {
    private static final String BASE_PROPS_FILE =
            "src/test/resources/XML/test-extended-env.properties";
    private static String encryptionKey = "21947a50-6755-47";
    private static String IV = "/NK/c+NKGUwMm0RF";
    private static String namespace = Test_ExtendedZConfigEnv.class.getCanonicalName();
    private static String zkLockName = "TEST_ZK_LOCK";
    private static String dbLockName = "TEST_DB_LOCK";
    private static String sqsQueueName = "TEST-SQS-QUEUE";

    @BeforeAll
    public static void setup() throws Exception {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(BASE_PROPS_FILE));
            String name = properties.getProperty(ConfigTestConstants.PROP_CONFIG_NAME);
            assertFalse(Strings.isNullOrEmpty(name));
            String filename = properties.getProperty(
                    ConfigTestConstants.PROP_CONFIG_FILE);
            assertFalse(Strings.isNullOrEmpty(filename));
            String vs = properties.getProperty(ConfigTestConstants.PROP_CONFIG_VERSION);
            assertFalse(Strings.isNullOrEmpty(vs));
            Version version = Version.parse(vs);
            assertNotNull(version);

            ExtendedZConfigEnv.setup(name, filename, ConfigProviderFactory.EConfigType.XML, vs, encryptionKey);
        } catch (Throwable t) {
            LogUtils.error(Test_ExtendedZConfigEnv.class, t);
            throw t;
        }
    }

    @AfterAll
    public static void dispose() throws Exception {
        ExtendedZConfigEnv.shutdown();
    }

    @Test
    void getDbLock() {
        try {
            DistributedLock lock = DistributedLockFactory.get().getDbLock(namespace, dbLockName);
            assertNotNull(lock);
            lock.lock();
            try {
                LogUtils.debug(getClass(), String.format("Acquired lock. [namespace=%s][name=%s]", lock.id().getNamespace(), lock.id().getName()));
            } finally {
                lock.unlock();
            }
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t);
        }
    }

    //@Test
    void getZkLock() {
        try {
            DistributedLock lock = DistributedLockFactory.get().getZkLock(namespace, zkLockName);
            assertNotNull(lock);
            lock.lock();
            try {
                LogUtils.debug(getClass(), String.format("Acquired lock. [namespace=%s][name=%s]", lock.id().getNamespace(), lock.id().getName()));
            } finally {
                lock.unlock();
            }
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t);
        }
    }

    @Test
    void testSqsQueueSend() {
        try {
            AbstractQueue<SQSConnection, DefaultStringMessage> queue = QueueManager.get().getQueue(sqsQueueName);
            assertNotNull(queue);
            DefaultStringMessage message = new DefaultStringMessage();
            message.setMessageId(UUID.randomUUID().toString());
            message.setBody(String.format("This is a test message. [id=%s]", message.getMessageId()));
            message.setTimestamp(System.currentTimeMillis());

            queue.send(message);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t);
        }
    }

    @Test
    void testSqsQueueReceive() {
        try {
            AbstractQueue<SQSConnection, DefaultStringMessage> queue = QueueManager.get().getQueue(sqsQueueName);
            assertNotNull(queue);
            for (int ii = 0; ii < 5; ii++) {
                DefaultStringMessage message = new DefaultStringMessage();
                message.setMessageId(UUID.randomUUID().toString());
                message.setBody(String.format("This is a test message. [id=%s]", message.getMessageId()));
                message.setTimestamp(System.currentTimeMillis());
                queue.send(message);
            }

            List<DefaultStringMessage> messages = queue.receiveBatch(5, 5000);
            assertNotNull(messages);
            assertTrue(messages.size() >= 5);

            for (int ii = 0; ii < messages.size(); ii++)
                LogUtils.debug(getClass(), String.format("[ID=%s] : %s", messages.get(ii).getMessageId(), messages.get(ii).getBody()));
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t);
        }
    }
}