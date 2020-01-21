package com.codekutter.r2db.driver;

import com.codekutter.common.ConfigTestConstants;
import com.codekutter.common.TestDataHelper;
import com.codekutter.common.stores.impl.RdbmsDataStore;
import com.codekutter.common.stores.model.*;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigProviderFactory;
import com.codekutter.zconfig.common.R2dbEnv;
import com.codekutter.zconfig.common.model.Version;
import com.google.common.base.Strings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class EntityManagerTest {

    private static final String BASE_PROPS_FILE =
            "src/test/resources/test-extended-env.properties";
    private static String encryptionKey = "21947a50-6755-47";
    private static String IV = "/NK/c+NKGUwMm0RF";
    private static EntityManager entityManager = null;
    private static final User user = new User(UUID.randomUUID().toString());

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

            R2dbEnv.setup(name, filename, ConfigProviderFactory.EConfigType.XML, vs, encryptionKey);
            entityManager = R2dbEnv.env().getEntityManager();
            assertNotNull(entityManager);
        } catch (Throwable t) {
            LogUtils.error(EntityManagerTest.class, t);
            throw t;
        }
    }

    @AfterAll
    static void shutdown() {
        try {
            R2dbEnv.shutdown();
        } catch (Throwable t) {
            LogUtils.error(EntityManagerTest.class, t);
            throw t;
        }
    }

    @Test
    void textSearch() {
    }

    @Test
    void testTextSearch() {
    }

    @Test
    void testTextSearch1() {
    }

    @Test
    void testTextSearch2() {
    }

    @Test
    void rollback() {
    }

    @Test
    void create() {
        try {
            entityManager.beingTransaction(Customer.class, RdbmsDataStore.class);
            try {
                List<Product> products = TestDataHelper.createProducts(5);
                for(Product p : products) {
                    entityManager.create(p, Product.class, RdbmsDataStore.class, user, null);
                }
                List<Order> orders = TestDataHelper.createData(products, 10);
                for (Order order : orders) {
                    entityManager.create(order, Order.class, RdbmsDataStore.class, user, null);
                }
                entityManager.commit(Customer.class, RdbmsDataStore.class);
            } catch (Throwable t) {
                entityManager.rollback(Customer.class, RdbmsDataStore.class);
                throw t;
            }
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t);
        }
    }

    @Test
    void update() {
    }

    @Test
    void delete() {
    }

    @Test
    void find() {
    }

    @Test
    void testFind() {
    }

    @Test
    void search() {
    }

    @Test
    void testSearch() {
    }

    @Test
    void testSearch1() {
    }

    @Test
    void testSearch2() {
    }

    @Test
    void testSearch3() {
    }

    @Test
    void testSearch4() {
    }

    @Test
    void testSearch5() {
    }

    @Test
    void testSearch6() {
    }
}