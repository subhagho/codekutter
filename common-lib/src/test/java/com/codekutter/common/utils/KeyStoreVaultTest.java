package com.codekutter.common.utils;

import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class KeyStoreVaultTest {
    private static final KeyStoreVault VAULT = new KeyStoreVault();

    @BeforeAll
    public static void setup() throws Exception {
        ConfigPathNode node = new ConfigPathNode();
        VAULT.configure(node);
    }

    @Test
    void addPasscode() {
        try {
            String keyname = "TEST_ADD_KEY";
            VAULT.addPasscode(keyname, "testp@ssw0rd".toCharArray());
            char[] pwd = VAULT.getPasscode(keyname);
            assertNotNull(pwd);
            assertTrue(pwd.length > 0);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t.getLocalizedMessage());
        }
    }

    @Test
    void getPasscode() {
        try {
            String keyname = "TEST_ADD_KEY";
            String pc = "testp@ssw0rd";

            VAULT.addPasscode(keyname, pc.toCharArray());
            char[] pwd = VAULT.getPasscode(keyname);
            assertNotNull(pwd);
            String ppc = new String(pwd);
            assertEquals(pc, ppc);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t.getLocalizedMessage());
        }
    }

    @Test
    void decrypt() {
        try {
            String keyname = "TEST_ADD_KEY";
            String pc = UUID.randomUUID().toString().substring(0, 16);

            String value = "Only thing that stinks about this is the fact that you still expose your keystore password. Would have to be stored in an environment variable or something that's user specific.";

            VAULT.addPasscode(keyname, pc.toCharArray());
            String iv = UUID.randomUUID().toString().substring(0, 16);
            String encrypted = CypherUtils.encryptAsString(value.getBytes(StandardCharsets.UTF_8), pc, iv);
            byte[] decrypted = CypherUtils.decrypt(
                    encrypted, pc, iv);
            assertNotNull(decrypted);
            assertTrue(decrypted.length > 0);
            String buff = new String(decrypted, StandardCharsets.UTF_8);
            assertEquals(value, buff);

            byte[] ret = VAULT.decrypt(encrypted, keyname, iv);
            assertNotNull(ret);
            String rv = new String(ret, StandardCharsets.UTF_8);
            assertEquals(value, rv);

        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t.getLocalizedMessage());
        }
    }

    @Test
    void decryptBytes() {
        try {
            String keyname = "TEST_ADD_KEY";
            String pc = UUID.randomUUID().toString().substring(0, 16);

            String value = "Only thing that stinks about this is the fact that you still expose your keystore password. Would have to be stored in an environment variable or something that's user specific.";

            VAULT.addPasscode(keyname, pc.toCharArray());
            String iv = UUID.randomUUID().toString().substring(0, 16);
            byte[] encrypted = CypherUtils.encrypt(value.getBytes(StandardCharsets.UTF_8), pc, iv);
            byte[] decrypted = CypherUtils.decrypt(
                    encrypted, pc, iv);
            assertNotNull(decrypted);
            assertTrue(decrypted.length > 0);
            String buff = new String(decrypted, StandardCharsets.UTF_8);
            assertEquals(value, buff);

            byte[] ret = VAULT.decrypt(encrypted, keyname, iv);
            assertNotNull(ret);
            String rv = new String(ret, StandardCharsets.UTF_8);
            assertEquals(value, rv);

        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t.getLocalizedMessage());
        }
    }
}
