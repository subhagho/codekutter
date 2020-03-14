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
 * Date: 10/2/19 8:22 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.utils;

import java.net.*;
import java.util.Enumeration;

/**
 * Network related utility function.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 */
public class NetUtils {
    /**
     * Get the non-loopback IP of the host. By default a v4 inet address will be
     * returned.
     *
     * @return - Non-loopback Inet Address.
     */
    public static InetAddress getIpAddress() {
        return getIpAddress(EAddressType.V4);
    }

    /**
     * Get the non-loopback IP of the host.
     *
     * @param type - Desired IP address type.
     * @return - Non-loopback Inet Address.
     */
    @SuppressWarnings("rawtypes")
    public static InetAddress getIpAddress(EAddressType type) {
        try {
            Enumeration e = NetworkInterface.getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface n = (NetworkInterface) e.nextElement();
                Enumeration ee = n.getInetAddresses();
                while (ee.hasMoreElements()) {
                    InetAddress i = (InetAddress) ee.nextElement();
                    if (i.getHostAddress().startsWith("127")
                            || !EAddressType.matches(type, i))
                        continue;
                    return i;
                }
            }
        } catch (Exception e) {
            LogUtils.error(NetUtils.class, e.getLocalizedMessage());
        }
        return null;
    }

    /**
     * Get the Hardware MAC address for the specified IP Address.
     *
     * @param ip - Interface IP address.
     * @return - MAC Address as String.
     * @throws SocketException
     */
    public static String getMacAddress(InetAddress ip) throws SocketException {
        NetworkInterface network = NetworkInterface.getByInetAddress(ip);
        byte[] mac = network.getHardwareAddress();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mac.length; i++) {
            sb.append(String.format("%02X%s", mac[i],
                    (i < mac.length - 1) ? "-" : ""));
        }

        return sb.toString();
    }

    public static enum EAddressType {
        /**
         * v4 IP address required.
         */
        V4,
        /**
         * v6 IP address required.
         */
        V6;

        /**
         * Check to see if the specified Inet address is of the desired type.
         *
         * @param type - Desired type.
         * @param addr - Inet Address.
         * @return - Is match?
         */
        public static boolean matches(EAddressType type, InetAddress addr) {
            if (type == V4) {
                if (addr instanceof Inet4Address)
                    return true;
            } else if (type == V6) {
                if (addr instanceof Inet6Address)
                    return true;
            }
            return false;
        }
    }
}
