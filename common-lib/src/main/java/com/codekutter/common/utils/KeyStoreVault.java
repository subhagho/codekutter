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

import com.codekutter.common.IKeyVault;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

public class KeyStoreVault implements IKeyVault {
    private static final String SK_FACTORY_ALGO = "PBE";
    private static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

    private KeyStore keyStore = null;
    private SecureRandom rand = new SecureRandom();
    private char[] array = null;

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        try {
            // keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
            array = generateSalt(16).toCharArray();
            keyStore.load(null, array);
        } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
            keyStore = null;
            throw new ConfigurationException(e);
        }
    }

    @Override
    public IKeyVault addPasscode(@Nonnull String name, @Nonnull char[] key, Object... params) throws SecurityException {
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance(SK_FACTORY_ALGO);
            SecretKey generatedSecret =
                    factory.generateSecret(new PBEKeySpec(key));
            KeyStore.PasswordProtection keyStorePP = new KeyStore.PasswordProtection(array);
            keyStore.setEntry(name, new KeyStore.SecretKeyEntry(
                    generatedSecret), keyStorePP);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | KeyStoreException e) {
            throw new SecurityException(e);
        }
        return this;
    }

    @Override
    public char[] getPasscode(@Nonnull String name) throws SecurityException {
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance(SK_FACTORY_ALGO);
            KeyStore.PasswordProtection keyStorePP = new KeyStore.PasswordProtection(array);
            KeyStore.SecretKeyEntry ske =
                    (KeyStore.SecretKeyEntry) keyStore.getEntry(name, keyStorePP);

            PBEKeySpec keySpec = (PBEKeySpec) factory.getKeySpec(
                    ske.getSecretKey(),
                    PBEKeySpec.class);
            return keySpec.getPassword();
        } catch (NoSuchAlgorithmException | UnrecoverableEntryException | KeyStoreException | InvalidKeySpecException e) {
            throw new SecurityException(e);
        }
    }

    @Override
    public byte[] decrypt(@Nonnull String data, @Nonnull String name, @Nonnull String iv) throws SecurityException {
        char[] pc = getPasscode(name);
        if (pc == null || pc.length <= 0) {
            throw new SecurityException(String.format("No key found for specified name. [name=%s]", name));
        }
        try {
            return CypherUtils.decrypt(data, new String(pc), iv);
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    @Override
    public byte[] decrypt(@Nonnull byte[] data, @Nonnull String name, @Nonnull String iv) throws SecurityException {
        char[] pc = getPasscode(name);
        if (pc == null || pc.length <= 0) {
            throw new SecurityException(String.format("No key found for specified name. [name=%s]", name));
        }
        try {
            return CypherUtils.decrypt(data, new String(pc), iv);
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }


    public String generateSalt(final int length) {
        Preconditions.checkArgument(length > 0);

        byte[] salt = new byte[length];
        rand.nextBytes(salt);

        return Base64.getEncoder().encodeToString(salt);
    }
}
