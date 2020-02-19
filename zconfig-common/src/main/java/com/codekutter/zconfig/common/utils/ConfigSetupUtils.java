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

import com.codekutter.common.utils.CypherUtils;
import com.codekutter.zconfig.common.ConfigKeyVault;
import com.google.common.base.Strings;
import org.kohsuke.args4j.*;

import java.io.Console;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ConfigSetupUtils {
    public enum EOperation {
        IV, encrypt, decrypt, hash
    }

    @Option(name = "-o", usage = "Operation to perform", aliases = {"--op"})
    private String op;
    @Option(name = "-i", usage = "Configuration ID", aliases = {"--id"})
    private String id;
    @Option(name = "-g", usage = "Configuration Application Group", aliases = {"--group"})
    private String group;
    @Option(name = "-a", usage = "Configuration Application Name", aliases = {"--application"})
    private String app;
    @Option(name = "-n", usage = "Configuration Name", aliases = {"--name"})
    private String name;
    @Option(name = "-h", usage = "Configuration Key Hash", aliases = {"--hash"})
    private String hash;
    @Option(name = "-k", usage = "Key to encrypt/decrypt with.", aliases = {"--key"})
    private String key;
    @Option(name = "-s", usage = "IV Spec to use to encryption/decryption.", aliases = {"--iv"})
    private String iv;
    @Argument
    private List<String> otherArgs = new ArrayList<>();

    private String input;
    private EOperation operation;

    private void execute(String[] args) throws Exception {
        ParserProperties props = ParserProperties.defaults().withUsageWidth(256);
        CmdLineParser parser = new CmdLineParser(this, props);

        String value = null;
        try {
            // parse the arguments.
            parser.parseArgument(args);

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
                value = CypherUtils.encryptAsString(input.getBytes(StandardCharsets.UTF_8), key, iv);
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
                value = new String(data, StandardCharsets.UTF_8);
            } else if (operation == EOperation.hash) {
                if (Strings.isNullOrEmpty(key)) {
                    key = getValue("Key");
                }
                text = "Generated Hash";
                value = CypherUtils.getKeyHash(key);
            }
            String output = String.format("%s: [%s]\n", text, value);
            System.out.println(output);
        } catch (CmdLineException e) {
            printUsage(parser, e);
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

    public static void main(String[] args) {
        try {
            new ConfigSetupUtils().execute(args);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
