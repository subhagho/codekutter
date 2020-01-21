package com.codekutter.common.stores;

import com.codekutter.common.ConfigTestConstants;
import com.codekutter.common.TestDataHelper;
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
            List<Order> orders = TestDataHelper.createData(1, 10);
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
            List<Order> orders = TestDataHelper.createData(5, 10);
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
}