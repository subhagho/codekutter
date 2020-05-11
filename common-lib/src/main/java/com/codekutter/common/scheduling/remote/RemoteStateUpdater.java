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

package com.codekutter.common.scheduling.remote;

import com.codekutter.common.auditing.AuditException;
import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.JobAuditLog;
import com.codekutter.common.scheduling.IJobAuditLogger;
import com.codekutter.common.scheduling.JobConfig;
import com.codekutter.common.scheduling.ScheduleManager;
import com.codekutter.common.scheduling.impl.AsyncRestJobConfig;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.impl.RestConnection;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.Runner;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.Strings;
import org.quartz.JobExecutionException;

import javax.annotation.Nonnull;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
@Accessors(fluent = true)
public class RemoteStateUpdater extends Runner implements IConfigurable {
    private static final int DEFAULT_FETCH_INTERVAL = 60 * 1000; // Every minute
    @ConfigValue
    private int fetchInterval = DEFAULT_FETCH_INTERVAL;
    @Setter(AccessLevel.NONE)
    private ScheduleManager scheduleManager;

    public RemoteStateUpdater withScheduleManager(@Nonnull ScheduleManager scheduleManager) {
        this.scheduleManager = scheduleManager;
        return this;
    }

    @Override
    public void doRun() throws Exception {
        try {
            Thread.sleep(DEFAULT_FETCH_INTERVAL);
            while (scheduleManager != null && scheduleManager.state().getState() == EObjectState.Available) {
                Set<JobConfig> configs = scheduleManager.getAsyncJobConfigs();
                if (configs != null && !configs.isEmpty()) {
                    processingPending(configs);
                    processingRunning(configs);
                }
                Thread.sleep(fetchInterval);
            }
            LogUtils.info(getClass(), "Shutting down Remote State Updater...");
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            throw new JobExecutionException(t);
        }
    }

    private void processingPending(Set<JobConfig> configs) throws JobExecutionException, AuditException, DataStoreException {
        IJobAuditLogger logger = scheduleManager.auditLogger();
        if (logger != null) {
            List<JobAuditLog> records = logger.findPendingJobs();
            if (records != null && !records.isEmpty()) {
                Multimap<JobConfig, JobAuditLog> recordMap = ArrayListMultimap.create();
                for (JobAuditLog record : records) {
                    JobConfig config = findConfig(configs, record);
                    if (config == null) {
                        LogUtils.warn(getClass(),
                                String.format("Job Config not found: [Job ID=%s][key=%s]",
                                        record.getJobId(), JobConfig.key(record.getNamespace(), record.getName())));
                        continue;
                    }
                    if (config.isAsync())
                        recordMap.put(config, record);
                }
                if (!recordMap.isEmpty()) {
                    for (JobConfig config : recordMap.keySet()) {
                        Collection<JobAuditLog> ars = recordMap.get(config);
                        if (ars.isEmpty()) continue;
                        JobStateResponse response = updateStates(config, ars, logger);
                        if (response != null) {
                            LogUtils.debug(getClass(), response);
                        } else {
                            LogUtils.error(getClass(),
                                    String.format("State response is NULL. [config=%s]",
                                            JobConfig.key(config.getNamespace(), config.getName())));
                        }
                    }
                }
            }
        }
    }

    private void processingRunning(Set<JobConfig> configs) throws JobExecutionException, AuditException, DataStoreException {
        IJobAuditLogger logger = scheduleManager.auditLogger();
        if (logger != null) {
            List<JobAuditLog> records = logger.findRunningJobs();
            if (records != null && !records.isEmpty()) {
                Multimap<JobConfig, JobAuditLog> recordMap = ArrayListMultimap.create();
                for (JobAuditLog record : records) {
                    JobConfig config = findConfig(configs, record);
                    if (config == null) {
                        LogUtils.warn(getClass(),
                                String.format("Job Config not found: [Job ID=%s][key=%s]",
                                        record.getJobId(), JobConfig.key(record.getNamespace(), record.getName())));
                        continue;
                    }
                    if (config.isAsync())
                        recordMap.put(config, record);
                }
                if (!recordMap.isEmpty()) {
                    for (JobConfig config : recordMap.keySet()) {
                        Collection<JobAuditLog> ars = recordMap.get(config);
                        if (ars.isEmpty()) continue;
                        JobStateResponse response = updateStates(config, ars, logger);
                        if (response != null) {
                            LogUtils.debug(getClass(), response);
                        } else {
                            LogUtils.error(getClass(),
                                    String.format("State response is NULL. [config=%s]",
                                            JobConfig.key(config.getNamespace(), config.getName())));
                        }
                    }
                }
            }
        }
    }

    private JobStateResponse updateStates(JobConfig config,
                                          Collection<JobAuditLog> records,
                                          IJobAuditLogger logger) throws JobExecutionException, DataStoreException, AuditException {
        if (config instanceof AsyncRestJobConfig) {
            AsyncRestJobConfig rc = (AsyncRestJobConfig) config;

            RestConnection connection = (RestConnection) ConnectionManager.get().connection(rc.getConnectionName(), Client.class);
            if (connection == null) {
                throw new JobExecutionException(String.format("Connection not found. [type=%s][name=%s]",
                        RestConnection.class.getCanonicalName(), rc.getConnectionName()));
            }
            JobStateRequest request = new JobStateRequest();
            request.setNodeId(scheduleManager.scheduleNodeId());
            request.setRequestTime(System.currentTimeMillis());
            for (JobAuditLog record : records) {
                if (Strings.isNullOrEmpty(record.getCorrelationId())) {
                    LogUtils.error(getClass(), String.format("Async request missing correlation ID. [jpb ID=%s]", record.getJobId()));
                    continue;
                }
                request.addCorrelationId(record.getCorrelationId());
            }
            WebTarget target = connection.client().target(rc.getRequestStatusUrl());
            Entity<JobStateRequest> re = Entity.entity(request, MediaType.APPLICATION_JSON_TYPE);
            Response response = target.request(rc.getMediaType()).put(re);
            if (response.getStatus() != HttpStatus.SC_OK) {
                throw new JobExecutionException(String.format("Request failed. [status=%d]", response.getStatus()));
            }
            JobStateResponse stateResponse = response.readEntity(JobStateResponse.class);
            if (stateResponse != null) {
                if (stateResponse.getStates() != null && !stateResponse.getStates().isEmpty()) {
                    Map<String, JobResponse> responses = stateResponse.getStates();
                    for (String key : responses.keySet()) {
                        JobResponse jr = responses.get(key);
                        if (jr.getJobState().hasError()) {
                            logger.logJobError(jr.getCorrelationId(), jr.getJobState().getError());
                        } else {
                            logger.logJobState(jr.getCorrelationId(), jr.getJobState().getState());
                        }
                    }
                }
            }
        }
        return null;
    }

    private JobConfig findConfig(Set<JobConfig> configs, JobAuditLog record) {
        if (configs != null && !configs.isEmpty()) {
            String key = JobConfig.key(record.getNamespace(), record.getName());
            for (JobConfig config : configs) {
                String ck = JobConfig.key(config.getNamespace(), config.getName());
                if (ck.compareTo(key) == 0) {
                    return config;
                }
            }
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
        if (node instanceof ConfigPathNode) {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
        }
    }
}
