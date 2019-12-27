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

import com.codekutter.common.utils.NetUtils;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.experimental.Accessors;

import java.net.InetAddress;
import java.net.SocketException;

@Data
@Accessors(fluent = true)
public class ServerInfo {
    private String hsotname;
    private String ipAddress;
    private String macAddress;

    public static ServerInfo get() throws SocketException {

        ServerInfo info = new ServerInfo();
        InetAddress addrs = NetUtils.getIpAddress(NetUtils.EAddressType.V4);
        Preconditions.checkState(addrs != null);
        info.ipAddress = addrs.toString();
        info.hsotname = addrs.getHostName();
        info.macAddress = NetUtils.getMacAddress(addrs);

        return info;
    }
}
