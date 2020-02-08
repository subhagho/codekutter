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

package com.codekutter.common.utils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CypherUtilsTest {

    @Test
    void decrypt() {
        try {
            String passcode = UUID.randomUUID().toString().substring(0, 16);
            String iv = UUID.randomUUID().toString().substring(0, 16);
            String data =
                    "I want to encrypt a string and then put it on a file. Also want to decrypt it when I want. I don’t need very strong security. I just want to make it harder to get my data others.";
            byte[] encrypted = CypherUtils.encrypt(data.getBytes(StandardCharsets.UTF_8), passcode, iv);
            assertNotNull(encrypted);
            assertTrue(encrypted.length > 0);

            String buff = new String(encrypted, StandardCharsets.UTF_8);
            LogUtils.debug(getClass(), String.format("Encrypted Data: [%s]", buff));

            byte[] decrypted = CypherUtils.decrypt(encrypted, passcode, iv);
            assertNotNull(decrypted);
            assertTrue(decrypted.length > 0);
            buff = new String(decrypted, StandardCharsets.UTF_8);
            assertEquals(data, buff);
            LogUtils.debug(getClass(), buff);
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex.getLocalizedMessage());
        }
    }

    @Test
    void decryptString() {
        try {
            String passcode = UUID.randomUUID().toString().substring(0, 16);
            String iv = UUID.randomUUID().toString().substring(0, 16);

            String data =
                    "I want to encrypt a string and then put it on a file. Also want to decrypt it when I want. I don’t need very strong security. I just want to make it harder to get my data others.";
            String encrypted =
                    CypherUtils.encryptAsString(data, passcode, iv);
            assertNotNull(encrypted);
            assertTrue(encrypted.length() > 0);

            LogUtils.debug(getClass(),
                           String.format("Encrypted Data: [%s]", encrypted));

            byte[] decrypted = CypherUtils.decrypt(
                    encrypted, passcode, iv);
            assertNotNull(decrypted);
            assertTrue(decrypted.length > 0);
            String buff = new String(decrypted, StandardCharsets.UTF_8);
            assertEquals(data, buff);
            LogUtils.debug(getClass(), buff);
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex.getLocalizedMessage());
        }
    }
}