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

import com.codekutter.common.auditing.AuditException;
import com.codekutter.common.scheduling.remote.EJobState;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.Monitoring;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Timer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.common.Strings;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AbstractJob implements Job {
    private final String METRIC_LATENCY_CALL = String.format("%s.%s.CALL", "%s", "%s");
    private final String METRIC_COUNTER_CALL = String.format("%s.%s.COUNT.CALL", "%s", "%s");
    private final String METRIC_COUNTER_ERROR_CALL = String.format("%s.%s.COUNT.ERRORS.CALL", "%s", "%s");
    /**
     * Metrics - Call Latency
     */
    @Setter(AccessLevel.NONE)
    protected Timer callLatency = null;
    /**
     * Counter - Call events
     */
    @Setter(AccessLevel.NONE)
    protected Id callCounter = null;
    /**
     * Counter - Call Error events
     */
    @Setter(AccessLevel.NONE)
    protected Id callErrorCounter = null;

    public AbstractJob() {
        setupMonitoring();
    }

    public void setupMonitoring() {
        String name = UUID.randomUUID().toString();

        callLatency = Monitoring.addTimer(String.format(METRIC_LATENCY_CALL, getClass().getCanonicalName(), name));
        callCounter = Monitoring.addCounter(String.format(METRIC_COUNTER_CALL, getClass().getCanonicalName(), name));
        callErrorCounter = Monitoring.addCounter(String.format(METRIC_COUNTER_ERROR_CALL, getClass().getCanonicalName(), name));
    }

    /**
     * <p>
     * Called by the <code>{@link ScheduleManager}</code> when a <code>{@link Trigger}</code>
     * fires that is associated with the <code>Job</code>.
     * </p>
     *
     * <p>
     * The implementation may wish to set a
     * {@link JobExecutionContext#setResult(Object) result} object on the
     * {@link JobExecutionContext} before this method exits.  The result itself
     * is meaningless to Quartz, but may be informative to
     * <code>{@link JobListener}s</code> or
     * <code>{@link TriggerListener}s</code> that are watching the job's
     * execution.
     * </p>
     *
     * @param context
     * @throws JobExecutionException if there is an exception while executing the job.
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            Monitoring.increment(callCounter.name(), (Map<String, String>) null);
            JobKey key = context.getJobDetail().getKey();
            JobConfig config = ScheduleManager.get(getClass()).getJobConfig(key.getGroup(), key.getName());
            if (config == null) {
                throw new JobExecutionException(String.format("[%s.%s] Failed to get job context.", key.getGroup(), key.getName()));
            }
            callLatency.record(() -> {
                IJobAuditLogger logger = null;
                String jobId = null;
                try {
                    if (config.isAudited()) {
                        logger = config.getManager().auditLogger();
                    }
                    String correlationId = UUID.randomUUID().toString();
                    correlationId = String.format("%s::%s", getClass().getName().toUpperCase(), correlationId);
                    if (logger != null) {
                        jobId = logger.logJobStart(config, correlationId, context, getClass());
                    }
                    Object response = doExecute(correlationId, context, config);
                    if (!config.isAsync() && logger != null) {
                        logger.logJobEnd(jobId, response, null);
                    }
                } catch (Throwable e) {
                    try {
                        if (logger != null && !Strings.isNullOrEmpty(jobId)) {
                            logger.logJobEnd(jobId, null, e);
                        }
                    } catch (AuditException ae) {
                        LogUtils.error(getClass(), ae);
                    }
                    Monitoring.increment(callErrorCounter.name(), (Map<String, String>) null);
                    LogUtils.error(getClass(), e);
                }
            });
        } catch (Exception ex) {
            Monitoring.increment(callErrorCounter.name(), (Map<String, String>) null);
            throw new JobExecutionException(ex);
        }
    }

    public void auditJobState(@Nonnull String correlationId, @Nonnull EJobState state) throws JobExecutionException {
        try {
            IJobAuditLogger logger = ScheduleManager.get(getClass()).auditLogger();
            if (logger != null) {
                logger.logJobState(correlationId, state);
            }
        } catch (Exception ex) {
            throw new JobExecutionException(ex);
        }
    }

    public void auditJobError(@Nonnull String correlationId, Throwable error) throws JobExecutionException {
        try {
            IJobAuditLogger logger = ScheduleManager.get(getClass()).auditLogger();
            if (logger != null) {
                logger.logJobError(correlationId, error);
            }
        } catch (Exception ex) {
            throw new JobExecutionException(ex);
        }
    }

    public abstract Object doExecute(@Nonnull String correlationId, @Nonnull JobExecutionContext context, @Nonnull JobConfig config) throws JobExecutionException;
}
