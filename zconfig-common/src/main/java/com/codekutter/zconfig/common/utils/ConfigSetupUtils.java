/*
 *  Copyright (2019) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
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

package com.codekutter.zconfig.common.utils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.codekutter.common.GlobalConstants;
import com.codekutter.common.utils.CypherUtils;
import com.codekutter.zconfig.common.ConfigKeyVault;
import com.google.common.base.Strings;

import java.io.Console;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ConfigSetupUtils {
    @Parameter(names = {"-o", "--op"}, description = "Operation to perform")
    private String op;
    @Parameter(names = {"-i", "--id"}, description = "Configuration ID")
    private String id;
    @Parameter(names = {"-g", "--group"}, description = "Configuration Application Group")
    private String group;
    @Parameter(names = {"-a", "--application"}, description = "Configuration Application Name")
    private String app;
    @Parameter(names = {"-n", "--name"}, description = "Configuration Name")
    private String name;
    @Parameter(names = {"-h", "--hash"}, description = "Configuration Key Hash")
    private String hash;
    @Parameter(names = {"-k", "--key"}, description = "Key to encrypt/decrypt with.")
    private String key;
    @Parameter(names = {"-s", "--iv"}, description = "IV Spec to use to encryption/decryption.")
    private String iv;
    @Parameter(names = {"--help"}, help = true)
    private boolean help = false;
    @Parameter(description = "Other program arguments...")
    private List<String> otherArgs = new ArrayList<>();
    private String input;
    private EOperation operation;

    public static void main(String[] args) {
        try {
            new ConfigSetupUtils().execute(args);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void execute(String[] args) throws Exception {
        JCommander parser = JCommander.newBuilder().addObject(this).build();

        String value = null;
        try {
            // parse the arguments.
            parser.parse(args);

            if (help) {
                parser.usage();
                return;
            }

            if (Strings.isNullOrEmpty(op)) {
                op = getValue("Operation");
            }
            operation = EOperation.valueOf(op);
            String text = null;
            if (operation == EOperation.IV) {
                if (Strings.isNullOrEmpty(id)) {
                    id = getValue("ID");
                }
                if (Strings.isNullOrEmpty(group)) {
                    group = getValue("Application Group");
                }
                if (Strings.isNullOrEmpty(app)) {
                    app = getValue("Application");
                }
                if (Strings.isNullOrEmpty(name)) {
                    name = getValue("Configuration Name");
                }
                if (Strings.isNullOrEmpty(hash)) {
                    hash = getValue("Configuration Hash Key");
                }
                text = "Generated IV Spec";
                value = ConfigKeyVault.getIvSpec(id, group, app, name, hash);
            } else if (operation == EOperation.encrypt) {
                if (Strings.isNullOrEmpty(iv)) {
                    iv = getValue("IV Spec");
                }
                if (!otherArgs.isEmpty()) {
                    input = otherArgs.get(0);
                }
                if (Strings.isNullOrEmpty(key)) {
                    key = getPassword();
                }
                text = "Encrypted Value";
                value = CypherUtils.encryptAsString(input.getBytes(GlobalConstants.defaultCharset()), key, iv);
            } else if (operation == EOperation.decrypt) {
                if (Strings.isNullOrEmpty(iv)) {
                    iv = getValue("IV Spec");
                }
                if (!otherArgs.isEmpty()) {
                    input = otherArgs.get(0);
                }
                if (Strings.isNullOrEmpty(key)) {
                    key = getPassword();
                }
                text = "Encrypted Value";
                byte[] data = CypherUtils.decrypt(input, key, iv);
                value = new String(data, GlobalConstants.defaultCharset());
            } else if (operation == EOperation.hash) {
                if (Strings.isNullOrEmpty(key)) {
                    key = getValue("Key");
                }
                text = "Generated Hash";
                value = CypherUtils.getKeyHash(key);
            }
            String output = String.format("%s: [%s]\n", text, value);
            System.out.println(output);
        } catch (ParameterException e) {
            parser.usage();
            throw e;
        }
    }

    private String getPassword() {
        if (Strings.isNullOrEmpty(key)) {
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
                key = new String(buff);
                break;
            }
        }
        return key;
    }

    private String getValue(String name) {
        Console console = System.console();
        String value = null;
        while (true) {
            char[] buff = console.readPassword("Enter %s:", name);
            if (buff == null || buff.length == 0) {
                continue;
            }
            if (buff.length != 16) {
                System.err.println("Invalid Password : Must be 16 characters.");
                continue;
            }
            value = new String(buff);
            break;
        }
        return value;
    }

    public enum EOperation {
        IV, encrypt, decrypt, hash
    }
}
