package com.codekutter.common.stores;

import com.codekutter.common.ConfigTestConstants;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.annotations.Reference;
import com.codekutter.common.stores.model.*;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.ConfigProviderFactory;
import com.codekutter.zconfig.common.R2dbEnv;
import com.codekutter.zconfig.common.model.Version;
import com.google.common.base.Strings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class JoinPredicateHelperTest {
    private static final String BASE_PROPS_FILE =
            "src/test/resources/test-extended-env.properties";
    private static String encryptionKey = "21947a50-6755-47";
    private static String IV = "/NK/c+NKGUwMm0RF";


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

            R2dbEnv.setup(name, filename, ConfigProviderFactory.EConfigType.XML, vs, encryptionKey);
        } catch (Throwable t) {
            LogUtils.error(JoinPredicateHelperTest.class, t);
            throw t;
        }
    }

    @Test
    void generateHibernateJoinQuery() {
        try {
            List<Order> orders = createData(1, 10);
            Field field = ReflectionUtils.findField(Order.class, "items");
            assertNotNull(field);
            Reference reference = field.getAnnotation(Reference.class);
            assertNotNull(reference);
            String query = JoinPredicateHelper.generateHibernateJoinQuery(reference, orders.get(0),
                    field, R2dbEnv.env().getEntityManager().dataStoreManager());
            assertFalse(Strings.isNullOrEmpty(query));
            LogUtils.debug(getClass(), query);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t);
        }
    }

    @Test
    void testGenerateHibernateJoinQuery() {
        try {
            List<Order> orders = createData(5, 10);
            Field field = ReflectionUtils.findField(Order.class, "items");
            assertNotNull(field);
            Reference reference = field.getAnnotation(Reference.class);
            assertNotNull(reference);
            String query = JoinPredicateHelper.generateHibernateJoinQuery(reference, orders,
                    field, R2dbEnv.env().getEntityManager().dataStoreManager());
            assertFalse(Strings.isNullOrEmpty(query));
            LogUtils.debug(getClass(), query);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t);
        }
    }

    @Test
    void generateSearchQuery() {
    }

    @Test
    void testGenerateSearchQuery() {
    }

    private static List<Order> createData(int count, int itemCount) {
        List<Order> orders = new ArrayList<>();
        List<Product> products = new ArrayList<>(itemCount);
        for (int ii = 0; ii < itemCount; ii++) {
            Product product = createProduct(ii);
            products.add(product);
        }
        for (int ii = 0; ii < count; ii++) {
            Order order = new Order();
            order.setId(UUID.randomUUID().toString());
            order.setCustomer(createCustomer());
            for(int jj=0; jj < itemCount; jj++) {
                Product product = products.get(jj);
                ItemId id = new ItemId();
                id.setOrderId(order.getId());
                id.setProductId(product.getId());
                Item item = new Item();
                item.setId(id);
                item.setQuantity(jj);
                item.setUnitPrice(product.getBasePrice());

                order.addItem(item);
            }
            orders.add(order);
        }
        return orders;
    }

    private static Customer createCustomer() {
        Customer customer = new Customer();
        customer.setId(UUID.randomUUID().toString());
        customer.setFirstName("First");
        customer.setLastName("Name");
        customer.setDateOfBirth(new Date());
        customer.setEmailId("first.name@email.com");
        customer.setPhoneNumber("+91 12345-67899");
        return customer;
    }

    private static Product createProduct(int index) {
        Random rnd = new Random(System.currentTimeMillis());
        Product product = new Product();
        product.setId(UUID.randomUUID().toString());
        product.setName(String.format("PRODUCT_%d", index));
        product.setDescription(String.format("This is a test product. [name=%s]", product.getName()));
        product.setBasePrice(rnd.nextDouble());
        product.setCreatedDate(System.currentTimeMillis());
        return product;
    }
}