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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.codec.binary.Base64;
import org.kohsuke.args4j.*;
import org.kohsuke.args4j.spi.BooleanOptionHandler;

import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.Console;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

public class CypherUtils {
    private static final String HASH_ALGO = "MD5";
    private static final String CIPHER_ALGO = "AES/CBC/PKCS5Padding";
    private static final String CIPHER_TYPE = "AES";
    @Option(name = "-h", usage = "Get the MD5 Hash",
            handler = BooleanOptionHandler.class,
            aliases = {"--hash"})
    private boolean doHash = false;
    @Option(name = "-e", usage = "Encrypt the passed String",
            handler = BooleanOptionHandler.class,
            aliases = {"--encrypt"})
    private boolean encrypt = false;
    @Option(name = "-d", usage = "Decrypt the passed String",
            handler = BooleanOptionHandler.class,
            aliases = {"--decrypt"})
    private boolean decrypt = false;
    @Option(name = "-p", usage = "Password used to encrypt/decrypt",
            aliases = {"--password"})
    private String password;
    @Option(name = "-i", usage = "IV Spec used to encrypt/decrypt",
            aliases = {"--iv"})
    private String ivSpec;
    @Argument
    private List<String> otherArgs = new ArrayList<>();

    /**
     * Get an MD5 hash of the specified key.
     *
     * @param key - Input Key.
     * @return - String value of the Hash
     * @throws Exception
     */
    public static String getKeyHash(@Nonnull String key) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));

        MessageDigest digest = MessageDigest.getInstance(HASH_ALGO);
        byte[] d = digest.digest(key.getBytes(StandardCharsets.UTF_8));
        d = Base64.encodeBase64(d);
        return new String(d, StandardCharsets.UTF_8);
    }

    /**
     * Get an MD5 hash of the specified byte array.
     *
     * @param data - Input byte array.
     * @return - String value of the Hash
     * @throws Exception
     */
    public static String getHash(@Nonnull byte[] data) throws Exception {
        Preconditions.checkArgument(data != null && data.length > 0);

        MessageDigest digest = MessageDigest.getInstance(HASH_ALGO);
        byte[] d = digest.digest(data);
        d = Base64.encodeBase64(d);
        return new String(d, StandardCharsets.UTF_8);
    }

    /**
     * Encrypt the passed data buffer using the passcode.
     *
     * @param data     - Data Buffer.
     * @param password - Passcode.
     * @param iv       - IV Key
     * @return - Encrypted Buffer.
     * @throws Exception
     */
    public static byte[] encrypt(@Nonnull byte[] data, @Nonnull String password, @Nonnull String iv)
            throws Exception {
        Preconditions.checkArgument(data != null && data.length > 0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(iv));

        Cipher cipher = getCipher(password, iv, Cipher.ENCRYPT_MODE);

        return cipher.doFinal(data);
    }

    /**
     * Encrypt the passed data buffer using the passcode.
     *
     * @param data     - Data Buffer.
     * @param password - Passcode.
     * @param iv       - IV Key
     * @return - Base64 encoded String.
     * @throws Exception
     */
    public static String encryptAsString(@Nonnull byte[] data,
                                         @Nonnull String password, @Nonnull String iv)
            throws Exception {
        Preconditions.checkArgument(data != null && data.length > 0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(iv));

        byte[] encrypted = encrypt(data, password, iv);
        return new String(Base64.encodeBase64(encrypted));
    }

    /**
     * Encrypt the passed data buffer using the passcode.
     *
     * @param data     - Data Buffer.
     * @param password - Passcode.
     * @param iv       - IV Key
     * @return - Base64 encoded String.
     * @throws Exception
     */
    public static String encryptAsString(@Nonnull String data,
                                         @Nonnull String password, @Nonnull String iv)
            throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(data));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(iv));

        byte[] encrypted = encrypt(data.getBytes(StandardCharsets.UTF_8), password, iv);
        return new String(Base64.encodeBase64(encrypted));
    }

    /**
     * Decrypt the data buffer using the passcode.
     *
     * @param data     - Encrypted Data buffer.
     * @param password - Passcode
     * @param iv       - IV Key
     * @return - Decrypted Data Buffer.
     * @throws Exception
     */
    public static byte[] decrypt(@Nonnull byte[] data, @Nonnull String password, @Nonnull String iv) throws Exception {
        Preconditions.checkArgument(data != null && data.length > 0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(iv));

        Cipher cipher = getCipher(password, iv, Cipher.DECRYPT_MODE);
        // decrypt the text

        return cipher.doFinal(data);
    }

    private static Cipher getCipher(String password, String iv, int mode) throws Exception {
        // Create key and cipher
        Key aesKey = new SecretKeySpec(password.getBytes(StandardCharsets.UTF_8), CIPHER_TYPE);
        IvParameterSpec ivspec = new IvParameterSpec(iv.getBytes(StandardCharsets.UTF_8));

        Cipher cipher = Cipher.getInstance(CIPHER_ALGO);
        cipher.init(mode, aesKey, ivspec);

        return cipher;
    }

    /**
     * Decrypt the string data using the passcode.
     *
     * @param data     - Encrypted String data.
     * @param password - Passcode
     * @param iv       - IV Key
     * @return - Decrypted Data Buffer.
     * @throws Exception
     */
    public static byte[] decrypt(@Nonnull String data, @Nonnull String password, @Nonnull String iv) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(data));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(iv));

        byte[] array = Base64.decodeBase64(data);
        return decrypt(array, password, iv);
    }

    public static void main(String[] args) {
        try {
            new CypherUtils().execute(args);
        } catch (Throwable t) {
            LogUtils.error(CypherUtils.class, t);
            t.printStackTrace();
        }
    }

    private void execute(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        // if you have a wider console, you could increase the value;
        // here 80 is also the default
        parser.setUsageWidth(80);

        String value = null;
        try {
            // parse the arguments.
            parser.parseArgument(args);

            // you can parse additional arguments if you want.
            // parser.parseArgument("more","args");

            // after parsing arguments, you should check
            // if enough arguments are given.
            if (otherArgs.isEmpty())
                throw new CmdLineException(parser, "No argument is given");
            value = otherArgs.get(0);
            if (Strings.isNullOrEmpty(value)) {
                throw new CmdLineException(parser,
                        "NULL/Empty value to Hash/Encrypt.");
            }

        } catch (CmdLineException e) {
            printUsage(parser, e);
            throw e;
        }
        if (encrypt) {
            String pwd = getPassword();
            String output =
                    encryptAsString(value.getBytes(StandardCharsets.UTF_8), pwd, ivSpec);
            System.out.println(String.format("Encrypted Text: %s", output));
        } else if (decrypt) {
            String pwd = getPassword();
            byte[] buff = decrypt(value.getBytes(StandardCharsets.UTF_8), pwd, ivSpec);
            String output = new String(buff, StandardCharsets.UTF_8);
            System.out.println(String.format("Decrypted Text: %s", output));
        } else if (doHash) {
            String output = getKeyHash(value);
            System.out.println(String.format("MD5 Hash: %s", output));
        } else {
            Exception e = new Exception("No valid option set.");
            printUsage(parser, e);
            throw e;
        }
    }

    private void printUsage(CmdLineParser parser, Exception e) {
        // if there's a problem in the command line,
        // you'll get this exception. this will report
        // an error message.
        System.err.println(e.getMessage());
        System.err.println(String.format("java %s [options...] arguments...",
                getClass().getCanonicalName()));
        // print the list of available options
        parser.printUsage(System.err);
        System.err.println();

        // print option sample. This is useful some time
        System.err.println(
                String.format("  Example: java %s",
                        getClass().getCanonicalName()) +
                        parser.printExample(
                                OptionHandlerFilter.ALL));
    }

    private String getPassword() {
        if (Strings.isNullOrEmpty(password)) {
            Console console = System.console();
            while (true) {
                char[] buff = console.readPassword("Enter Password:");
                if (buff == null || buff.length == 0) {
                    continue;
                }
                if (buff.length != 16) {
                    System.err.println("Invalid Password : Must be 16 characters.");
                    continue;
                }
                password = new String(buff);
                break;
            }
        }
        return password;
    }
}
