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

package com.codekutter.common.model;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.net.SocketException;
import java.util.UUID;

@Data
@Accessors(fluent = true)
public class App {
    private String group;
    private String name;
    @Setter(AccessLevel.NONE)
    private String instanceId;
    private long starttime;
    private ServerInfo serverInfo;

    public App() {
        try {
            instanceId = UUID.randomUUID().toString();
            serverInfo = ServerInfo.get();
            starttime = System.currentTimeMillis();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }
}
