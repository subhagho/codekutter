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

package com.codekutter.common.scheduling;

import com.codekutter.common.StateException;
import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.ObjectState;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.common.Strings;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.quartz.DateBuilder.futureDate;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "schedule")
public class ScheduleManager implements IConfigurable, Closeable {
    public static final String CONFIG_PATH_JOBS = "jobs";
    public static final int DEFAULT_STARTUP_DELAY = 30;

    @Setter(AccessLevel.NONE)
    private Map<String, JobConfig> jobs = new HashMap<>();
    @Setter(AccessLevel.NONE)
    private Map<String, Trigger> triggers = new HashMap<>();
    @ConfigValue(required = true)
    private String quartzConfig;
    @ConfigValue
    private int startUpDelay = DEFAULT_STARTUP_DELAY;

    @Setter(AccessLevel.NONE)
    private ObjectState state = new ObjectState();

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private SchedulerFactory schedulerFactory = null;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Scheduler scheduler = null;

    public JobConfig getJobConfig(@Nonnull String namespace, @Nonnull String name) {
        String key = JobConfig.key(namespace, name);
        if (jobs.containsKey(key)) {
            return jobs.get(key);
        }
        return null;
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            schedulerFactory = new StdSchedulerFactory(quartzConfig);
            scheduler = schedulerFactory.getScheduler();
            AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (cnode != null) {
                AbstractConfigNode jnode = cnode.find(CONFIG_PATH_JOBS);
                if (jnode instanceof ConfigPathNode) {
                    readJobConfig((ConfigPathNode) jnode);
                } else if (jnode instanceof ConfigListElementNode) {
                    List<ConfigElementNode> nodes = ((ConfigListElementNode) jnode).getValues();
                    if (nodes != null && !nodes.isEmpty()) {
                        for (ConfigElementNode nn : nodes) {
                            readJobConfig((ConfigPathNode) nn);
                        }
                    }
                }
            }
            scheduler.start();
            Date startTime = futureDate(startUpDelay, DateBuilder.IntervalUnit.SECOND);
            for (String key : jobs.keySet()) {
                JobConfig config = jobs.get(key);
                Trigger trigger = scheduleJob(config, startTime);
                triggers.put(config.jobKey(), trigger);
            }
            state.setState(EObjectState.Available);
        } catch (Exception ex) {
            state.setError(ex);
            LogUtils.error(getClass(), ex);
            throw new ConfigurationException(ex);
        }
    }

    private Trigger scheduleJob(JobConfig config, Date start) throws SchedulerException {
        JobDetail jd = JobBuilder.newJob(config.getType()).withIdentity(config.getName(), config.getNamespace()).build();
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity(config.getName(), config.getNamespace())
                .startAt(start)
                .withPriority(config.getPriority())
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInSeconds(config.getScheduleInterval()))
                .build();
        scheduler.scheduleJob(jd, trigger);

        return trigger;
    }

    @SuppressWarnings("unchecked")
    private void readJobConfig(ConfigPathNode node) throws ConfigurationException {
        AbstractConfigNode jnode = ConfigUtils.getPathNode(JobConfig.class, node);
        if (jnode instanceof ConfigPathNode) {
            String cname = ConfigUtils.getClassAttribute(jnode);
            if (Strings.isNullOrEmpty(cname)) {
                throw new ConfigurationException(String.format("Error getting class attribute. [path=%s]", jnode.getAbsolutePath()));
            }
            try {
                Class<? extends JobConfig> cls = (Class<? extends JobConfig>) Class.forName(cname);
                JobConfig job = cls.newInstance();
                job.configure(jnode);

                jobs.put(job.jobKey(), job);
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (state.getState() == EObjectState.Available) {
                state.setState(EObjectState.Disposed);
                if (scheduler != null) {
                    scheduler.shutdown(true);
                }
            }
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new IOException(ex);
        }
    }

    private static final ScheduleManager __instance = new ScheduleManager();

    public static void setup(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        __instance.configure(node);
    }

    public static ScheduleManager get(Class<?> caller) throws StateException {
        __instance.state.check(EObjectState.Available, caller);
        return __instance;
    }

    public static void dispose() throws IOException {
        __instance.close();
    }
}
