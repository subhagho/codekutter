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

            R2dbEnv.setup(R2dbEnv.class, name, filename, ConfigProviderFactory.EConfigType.XML, vs, encryptionKey);
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
    void create() {
        try {
            entityManager.beingTransaction(Customer.class, RdbmsDataStore.class);
            try {
                List<Product> products = TestDataHelper.createProducts(5, null);
                for(Product p : products) {
                    entityManager.create(p, Product.class, RdbmsDataStore.class, user, null);
                }
                List<Order> orders = TestDataHelper.createData(products, 10);
                for (Order order : orders) {
                    entityManager.create(order, Order.class, RdbmsDataStore.class, user, null);
                }
                entityManager.commit();
            } catch (Throwable t) {
                entityManager.rollback();
                throw t;
            } finally {
                entityManager.closeStores();
            }
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t);
        }
    }

    @Test
    void update() {
        try {
            String prefix = UUID.randomUUID().toString();
            List<Order> orders = createOrders(prefix, 5, 3);
            assertNotNull(orders);
            assertEquals(3, orders.size());

            Order order = entityManager.find(orders.get(2).getKey(), Order.class, RdbmsDataStore.class, null);
            assertNotNull(order);
            List<Item> items = order.getItems();
            assertNotNull(items);
            assertTrue(items.size() > 0);
            for(Item item : items) {
                assertTrue(item.getQuantity() > 0);
            }
            items.remove(0);
            entityManager.beingTransaction(Customer.class, RdbmsDataStore.class);
            try {
                entityManager.update(order, Order.class, RdbmsDataStore.class, user,null);
                entityManager.commit();
            } catch (Throwable t) {
                entityManager.rollback();
                throw t;
            } finally {
                entityManager.closeStores();
            }
            LogUtils.debug(getClass(), order);
        } catch (Throwable t) {
            LogUtils.debug(getClass(), t);
            fail(t);
        }
    }

    @Test
    void delete() {
    }

    @Test
    void find() {
        try {
            String prefix = UUID.randomUUID().toString();
            List<Order> orders = createOrders(prefix, 1010, 3);
            assertNotNull(orders);
            assertEquals(3, orders.size());

            Order order = entityManager.find(orders.get(2).getKey(), Order.class, RdbmsDataStore.class, null);
            assertNotNull(order);
            List<Item> items = order.getItems();
            assertNotNull(items);
            assertEquals(1009, items.size());
            for(Item item : items) {
                assertTrue(item.getQuantity() > 0);
            }
            LogUtils.debug(getClass(), order);
        } catch (Throwable t) {
            LogUtils.debug(getClass(), t);
            fail(t);
        }
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

    private List<Order> createOrders(String productPrefix, int count, int orderCount) throws Exception {
        try {
            entityManager.beingTransaction(Customer.class, RdbmsDataStore.class);
            try {
                List<Product> products = TestDataHelper.createProducts(count, productPrefix);
                for(Product p : products) {
                    entityManager.create(p, Product.class, RdbmsDataStore.class, user, null);
                }
                List<Order> orders = TestDataHelper.createData(products, orderCount);
                for (Order order : orders) {
                    entityManager.create(order, Order.class, RdbmsDataStore.class, user, null);
                }
                entityManager.commit();

                return orders;
            } catch (Throwable t) {
                entityManager.rollback();
                throw t;
            } finally {
                entityManager.closeStores();
            }
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            throw t;
        }
    }
}