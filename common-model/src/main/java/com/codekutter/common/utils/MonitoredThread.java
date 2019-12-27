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

package com.codekutter.common.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class MonitoredThread extends Thread {

    public MonitoredThread() {

    }

    public MonitoredThread(Runnable target) {
        super(target);

    }

    public MonitoredThread(ThreadGroup group, Runnable target) {
        super(group, target);
    }

    public MonitoredThread(String name) {
        super(name);
    }

    public MonitoredThread(ThreadGroup group, String name) {
        super(group, name);
    }

    public MonitoredThread(Runnable target, String name) {
        super(target, name);
    }

    public MonitoredThread(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
    }

    public MonitoredThread(ThreadGroup group, Runnable target, String name, long stackSize) {
        super(group, target, name, stackSize);
    }

    public ThreadInfo getThreadInfo() {
        ThreadMXBean tmb = ManagementFactory.getThreadMXBean();
        return tmb.getThreadInfo(getId());
    }
}
