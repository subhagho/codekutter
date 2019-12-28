/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright (c) $year
 * Date: 2/1/19 2:38 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.utils.LogUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

public class TestJsonTree {
    private static final String JSON_FILE = "zconfig-common/src/test/resources/json/test-tree.json";

    private void parse() throws Exception {
        File pwd = new File(".");
        LogUtils.info(getClass(), "Current directory = " + pwd.getAbsolutePath());

        File file = new File(JSON_FILE);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(file);

        print("ROOT", rootNode, 0);
    }

    private void print(String name, JsonNode node, int offset) {
        String padding = Strings.repeat(" ", offset);
        String mesg = String.format("%s{NAME=%s, TYPE=%s} => %s", padding, name,
                                    node.getNodeType().name(), node.textValue());
        LogUtils.info(getClass(), mesg);

        Iterator<Map.Entry<String, JsonNode>> nodes = node.fields();
        if (nodes != null) {
            while (nodes.hasNext()) {
                Map.Entry<String, JsonNode> nn = nodes.next();
                print(nn.getKey(), nn.getValue(), offset + 1);
            }
        }
    }

    public static void main(String[] args) {
        try {
            new TestJsonTree().parse();
        } catch (Throwable e) {
            LogUtils.error(TestJsonTree.class, e);
        }
    }
}
