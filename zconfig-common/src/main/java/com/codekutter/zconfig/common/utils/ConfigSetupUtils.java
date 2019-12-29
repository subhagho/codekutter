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

import com.codekutter.zconfig.common.ConfigKeyVault;
import com.google.common.base.Strings;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;

import java.io.Console;
import java.nio.charset.StandardCharsets;

public class ConfigSetupUtils {
    @Option(name = "-i", usage = "Configuration ID", aliases = {"--id"})
    private String id;
    @Option(name = "-g", usage = "Configuration Application Group", aliases = {"--group"})
    private String group;
    @Option(name = "-a", usage = "Configuration Application Name", aliases = {"--application"})
    private String app;
    @Option(name = "-n", usage = "Configuration Name", aliases = {"--name"})
    private String name;
    @Option(name = "-h", usage = "Configuration Key Hash", aliases = {"--hash"})
    private String key;


    private void execute(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        // if you have a wider console, you could increase the value;
        // here 80 is also the default
        parser.setUsageWidth(80);

        String value = null;
        try {
            // parse the arguments.
            parser.parseArgument(args);

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
            if (Strings.isNullOrEmpty(key)) {
                key = getValue("Configuration Hash Key");
            }
            value = ConfigKeyVault.getIvSpec(id, group, app, name, key);
            System.out.println("Generated IV Spec: [" + value + "]\n");
        } catch (CmdLineException e) {
            printUsage(parser, e);
            throw e;
        }
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
