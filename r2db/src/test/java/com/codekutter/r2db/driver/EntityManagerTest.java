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

package com.codekutter.r2db.driver;

import com.codekutter.common.ConfigTestConstants;
import com.codekutter.common.TestDataHelper;
import com.codekutter.common.stores.BaseSearchResult;
import com.codekutter.common.stores.impl.EntitySearchResult;
import com.codekutter.common.stores.model.*;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.r2db.driver.impl.SearchableRdbmsDataStore;
import com.codekutter.zconfig.common.ConfigProviderFactory;
import com.codekutter.zconfig.common.R2dbEnv;
import com.codekutter.zconfig.common.model.Version;
import com.google.common.base.Strings;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
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

            Thread.sleep(15000);
        } catch (Throwable t) {
            LogUtils.error(EntityManagerTest.class, t);
            throw t;
        }
    }

    @AfterAll
    static void shutdown() {
        try {
            Thread.sleep(10000);
            R2dbEnv.shutdown();
        } catch (Throwable t) {
            LogUtils.error(EntityManagerTest.class, t);
            throw new RuntimeException(t);
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
            try {
                List<Product> products = TestDataHelper.createProducts(5, null);
                for (Product p : products) {
                    entityManager.create(p, Product.class, user, null, SearchableRdbmsDataStore.class);
                }
                List<Order> orders = TestDataHelper.createData(products, 10);
                for (Order order : orders) {
                    entityManager.create(order, Order.class, user, null, SearchableRdbmsDataStore.class);
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

            Order order = entityManager.find(orders.get(2).getKey(), Order.class, SearchableRdbmsDataStore.class, null);
            assertNotNull(order);
            List<Item> items = order.getItems();
            assertNotNull(items);
            assertTrue(items.size() > 0);
            for (Item item : items) {
                assertTrue(item.getQuantity() > 0);
            }
            items.remove(0);
            try {
                entityManager.update(order, Order.class, user, null, SearchableRdbmsDataStore.class);
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

            Order order = entityManager.find(orders.get(2).getKey(), Order.class, SearchableRdbmsDataStore.class, null);
            assertNotNull(order);
            List<Item> items = order.getItems();
            assertNotNull(items);
            assertEquals(1009, items.size());
            for (Item item : items) {
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
        try {
            String prefix = UUID.randomUUID().toString();
            List<Order> orders = createOrders(prefix, 100, 3);
            assertNotNull(orders);
            assertEquals(3, orders.size());


            LuceneQueryBuilder<Order> builder = LuceneQueryBuilder.builder(Order.class);
            SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
            Query query = builder
                    .range("items.quantity", "5", "*")
                    .term("items.id.productId", new String[]{EntityQueryBuilder.string(orders.get(0).getItems().get(5).getId().getProductId()),
                            EntityQueryBuilder.string(orders.get(0).getItems().get(8).getId().getProductId()),
                            EntityQueryBuilder.string(orders.get(0).getItems().get(35).getId().getProductId())})
                    .build();
            LogUtils.debug(getClass(), String.format("query=[%s]", query));
            BaseSearchResult<Order> result = entityManager.textSearch(query, Order.class, SearchableRdbmsDataStore.class, null);
            assertTrue(result instanceof EntitySearchResult);
            EntitySearchResult<Order> or = (EntitySearchResult<Order>) result;
            assertFalse(or.entities().isEmpty());
            for (Order order : or.entities()) {
                LogUtils.debug(getClass(), order);
            }
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex);
        }
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
            entityManager.beingTransaction(Customer.class, SearchableRdbmsDataStore.class);
            try {
                List<Product> products = TestDataHelper.createProducts(count, productPrefix);
                for (Product p : products) {
                    entityManager.create(p, Product.class, user, null, SearchableRdbmsDataStore.class);
                }
                List<Order> orders = TestDataHelper.createData(products, orderCount);
                for (Order order : orders) {
                    entityManager.create(order, Order.class, user, null, SearchableRdbmsDataStore.class);
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