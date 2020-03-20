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

package com.codekutter.common.stores;

import com.amazonaws.services.s3.AmazonS3;
import com.codekutter.common.ConfigTestConstants;
import com.codekutter.common.auditing.AuditManager;
import com.codekutter.common.stores.model.User;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.r2db.driver.EntityManager;
import com.codekutter.r2db.driver.impl.AwsS3Connection;
import com.codekutter.r2db.driver.impl.AwsS3ConnectionConfig;
import com.codekutter.r2db.driver.impl.AwsS3DataStore;
import com.codekutter.r2db.driver.impl.S3StoreConfig;
import com.codekutter.zconfig.common.ConfigProviderFactory;
import com.codekutter.zconfig.common.R2dbEnv;
import com.codekutter.zconfig.common.model.Version;
import com.google.common.base.Strings;
import org.hibernate.Session;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DataStoreManagerTest {
    private static final String BASE_PROPS_FILE =
            "src/test/resources/test-extended-env.properties";
    private static final User user = new User(UUID.randomUUID().toString());
    private static String encryptionKey = "21947a50-6755-47";
    private static String IV = "/NK/c+NKGUwMm0RF";
    private static EntityManager entityManager = null;

    @BeforeAll
    static void setup() throws Exception {
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

            R2dbEnv.setup(R2dbEnv.class, name, filename, ConfigProviderFactory.EConfigType.XML, vs, encryptionKey);
            entityManager = R2dbEnv.env().getEntityManager();
            assertNotNull(entityManager);

            Thread.sleep(5000);
        } catch (Throwable t) {
            LogUtils.error(DataStoreManagerTest.class, t);
            throw t;
        }
    }

    @AfterAll
    static void shutdown() {
        try {
            Thread.sleep(1000);
            AuditManager.get().flush();
            R2dbEnv.shutdown();
        } catch (Throwable t) {
            LogUtils.error(DataStoreManagerTest.class, t);
            throw new RuntimeException(t);
        }
    }

    @Test
    void readDynamicDConfig() {
        try {
            String dbConnection = "TestDbConnection";
            AbstractConnection<Session> connection = ConnectionManager.get().connection(dbConnection, Session.class);
            assertNotNull(connection);
            List<AbstractConnection<AmazonS3>> connections = ConnectionManager.get()
                    .readConnections(AwsS3ConnectionConfig.class, AwsS3Connection.class, connection.connection(), null);
            assertNotNull(connections);
            assertTrue(connections.size() > 0);
            for(AbstractConnection<AmazonS3> conn : connections) {
                LogUtils.info(getClass(), String.format("Connection: [name=%s]", conn.name()));
            }
            Set<String> dataStores = R2dbEnv.env().getEntityManager().dataStoreManager()
                    .readDynamicDConfig(connection.connection(), AwsS3DataStore.class, S3StoreConfig.class, null);
            assertNotNull(dataStores);
            assertTrue(dataStores.size() > 0);
            for (String ds : dataStores) {
                AwsS3DataStore dataStore = (AwsS3DataStore) R2dbEnv.env().getEntityManager().dataStoreManager().getDataStore(ds, AwsS3DataStore.class);
                LogUtils.info(getClass(), String.format("Data Store: [name=%s]", dataStore.name()));
            }
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t);
        }
    }
}