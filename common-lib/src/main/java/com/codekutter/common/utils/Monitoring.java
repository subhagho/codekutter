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

import com.codahale.metrics.*;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.netflix.spectator.api.*;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.gc.GcLogger;
import com.netflix.spectator.jvm.Jmx;
import com.netflix.spectator.metrics3.MetricsRegistry;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Monitoring {
    public static final int REPORTER_JMX = (int)Math.pow(2, 1);
    public static final int REPORTER_CSV = (int)Math.pow(2,2);
    public static final int REPORTER_SLF4J = (int)Math.pow(2,3);
    public static boolean enableGcStats = true;
    public static boolean enableMemoryStats = true;
    private static final int REPORT_INTERVAL = 10;

    private static final Registry __REGISTRY = new MetricsRegistry();
    private static Map<String, Id> counters = new ConcurrentHashMap<>();
    private static Map<String, Timer> timers = new ConcurrentHashMap<>();
    private static Map<String, DistributionSummary> distributionSummaries = new ConcurrentHashMap<>();
    private static GcLogger gcLogger = null;

    private static List<Reporter> reporters = new ArrayList<>();

    public static void start(int reps, String metricsDir, boolean memStats, boolean gcStats) throws ConfigurationException {
        if ((reps & REPORTER_JMX) > 0) {
            JmxReporter reporter = JmxReporter.forRegistry((MetricRegistry) __REGISTRY).build();
            reporter.start();

            reporters.add(reporter);
        }
        if ((reps & REPORTER_CSV) > 0) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(metricsDir));
            File dir = new File(metricsDir);
            if (!dir.exists()) {
                if (!dir.mkdirs()) {
                    throw new ConfigurationException(String.format("Error creating metrics directory. [path=%s]", dir.getAbsolutePath()));
                }
            }
            CsvReporter reporter = CsvReporter.forRegistry((MetricRegistry) __REGISTRY).build(dir);
            reporter.start(REPORT_INTERVAL, TimeUnit.SECONDS);

            reporters.add(reporter);
        }
        if ((reps & REPORTER_SLF4J) > 0) {
            Slf4jReporter reporter = Slf4jReporter.forRegistry((MetricRegistry) __REGISTRY).build();
            reporter.start(REPORT_INTERVAL, TimeUnit.SECONDS);

            reporters.add(reporter);
        }
        if (enableGcStats = gcStats) {
            gcLogger = new GcLogger();
            gcLogger.start(null);
        }
        if (enableMemoryStats = memStats) {
            Jmx.registerStandardMXBeans(__REGISTRY);
        }
    }

    public static void stop() {
        if (!reporters.isEmpty()) {
            for(Reporter r : reporters) {
                if (r instanceof JmxReporter) {
                    ((JmxReporter) r).stop();
                } else if (r instanceof CsvReporter) {
                    ((CsvReporter) r).stop();
                } else if (r instanceof Slf4jReporter) {
                    ((Slf4jReporter) r).stop();
                }
            }
        }
        if (enableGcStats && gcLogger != null) {
            gcLogger.stop();
        }
    }

    public static Id addCounter(@Nonnull String name) {
        Id id = __REGISTRY.createId(name);
        counters.put(name, id);

        return id;
    }

    public static Id addCounter(@Nonnull String name, String... tags) {
        Id id = __REGISTRY.createId(name, tags);
        counters.put(name, id);

        return id;
    }

    public static Timer addTimer(@Nonnull String name) {
        Timer timer = __REGISTRY.timer(name);
        timers.put(name, timer);

        return timer;
    }

    public static Timer addTimer(@Nonnull String name, String... tags) {
        Timer timer = __REGISTRY.timer(name, tags);
        timers.put(name, timer);

        return timer;
    }

    public static DistributionSummary addDistributionSummary(@Nonnull String name) {
        DistributionSummary summary = __REGISTRY.distributionSummary(name);
        distributionSummaries.put(name, summary);

        return summary;
    }

    public static DistributionSummary addDistributionSummary(@Nonnull String name, String... tags) {
        DistributionSummary summary = __REGISTRY.distributionSummary(name, tags);
        distributionSummaries.put(name, summary);

        return summary;
    }

    public static void addGauge(@Nonnull String name, @Nonnull Object source, @Nonnull String method) {
        __REGISTRY.methodValue(name, source, method);
    }

    public static void increment(@Nonnull String name, Map<String, String> tags) {
        if (counters.containsKey(name)) {
            Id id = counters.get(name);
            Id contId = id;
            if (tags != null && !tags.isEmpty()) {
                for(String tag : tags.keySet()) {
                    contId = contId.withTag(tag, tags.get(tag));
                }
            }
            __REGISTRY.counter(contId).increment();
        }
    }

    public static void summary(@Nonnull String name, long value) {
        if (distributionSummaries.containsKey(name)) {
            distributionSummaries.get(name).record(value);
        }
    }

    public static Timer timer(@Nonnull String name) {
        if (timers.containsKey(name)) {
            return timers.get(name);
        }
        return null;
    }
}
