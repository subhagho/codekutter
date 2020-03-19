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

import com.codekutter.zconfig.common.BaseConfigEnv;
import com.codekutter.zconfig.common.ConfigKeyVault;
import com.google.common.base.Strings;

import java.nio.charset.Charset;

public class ConfigCryptoHandler implements ICryptoHandler {
    @Override
    public byte[] encrypt(String value, Charset charset) throws CryptoException {
        if (!Strings.isNullOrEmpty(value)) {
            String e = encryptAsString(value, charset);
            return e.getBytes(charset);
        }
        return null;
    }

    @Override
    public byte[] encrypt(byte[] value) throws CryptoException {
        if (value != null && value.length > 0) {
            String v = new String(value);
            String e = encryptAsString(v, Charset.defaultCharset());
            return e.getBytes(Charset.defaultCharset());
        }
        return null;
    }

    @Override
    public String encryptAsString(String value, Charset charset) throws CryptoException {
        if (!Strings.isNullOrEmpty(value)) {
            try {
                ConfigKeyVault vault = ConfigKeyVault.getInstance();
                if (vault == null) {
                    throw new Exception("Configuration Key Vault not registered.");
                }
                return vault.encrypt(value, BaseConfigEnv.env().getConfiguration());
            } catch (Exception ex) {
                throw new CryptoException(ex);
            }
        }
        return null;
    }

    @Override
    public byte[] decrypt(String encrypted, Charset charset) throws CryptoException {
        if (!Strings.isNullOrEmpty(encrypted)) {
            try {
                ConfigKeyVault vault = ConfigKeyVault.getInstance();
                if (vault == null) {
                    throw new Exception("Configuration Key Vault not registered.");
                }

                String value = vault.decrypt(encrypted, BaseConfigEnv.env().getConfiguration());
                if (Strings.isNullOrEmpty(value)) {
                    throw new Exception(
                            "Error Decrypting Value. NULL value returned.");
                }
                return value.getBytes(Charset.defaultCharset());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return null;
    }

    @Override
    public byte[] decrypt(byte[] encrypted) throws CryptoException {
        if (encrypted != null && encrypted.length > 0) {
            try {
                ConfigKeyVault vault = ConfigKeyVault.getInstance();
                if (vault == null) {
                    throw new Exception("Configuration Key Vault not registered.");
                }
                String str = new String(encrypted);
                String value = vault.decrypt(str, BaseConfigEnv.env().getConfiguration());
                if (Strings.isNullOrEmpty(value)) {
                    throw new Exception(
                            "Error Decrypting Value. NULL value returned.");
                }
                return value.getBytes(Charset.defaultCharset());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return null;
    }

    @Override
    public String decryptAsString(String encrypted, Charset charset) throws CryptoException {
        if (!Strings.isNullOrEmpty(encrypted)) {
            try {
                ConfigKeyVault vault = ConfigKeyVault.getInstance();
                if (vault == null) {
                    throw new Exception("Configuration Key Vault not registered.");
                }

                String value = vault.decrypt(encrypted, BaseConfigEnv.env().getConfiguration());
                if (Strings.isNullOrEmpty(value)) {
                    throw new Exception(
                            "Error Decrypting Value. NULL value returned.");
                }
                return value;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return encrypted;
    }
}
