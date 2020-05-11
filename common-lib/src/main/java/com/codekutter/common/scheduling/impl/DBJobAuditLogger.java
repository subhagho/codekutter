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

package com.codekutter.common.scheduling.impl;

import com.codekutter.common.GlobalConstants;
import com.codekutter.common.auditing.AuditException;
import com.codekutter.common.model.JobAuditLog;
import com.codekutter.common.scheduling.AbstractJob;
import com.codekutter.common.scheduling.IJobAuditLogger;
import com.codekutter.common.scheduling.JobConfig;
import com.codekutter.common.scheduling.remote.EJobState;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.stores.impl.HibernateConnection;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.quartz.JobExecutionContext;

import javax.annotation.Nonnull;
import javax.persistence.Query;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "logger")
public class DBJobAuditLogger implements IJobAuditLogger {
    private static final int MAX_ERROR_TEXT = 256;

    @ConfigAttribute(name = "connection", required = true)
    private String connectionName;

    /**
     * Log Job start.
     *
     * @param config  - Job Config
     * @param context - Execution Context
     * @param type    - Job Type
     * @return - Unique Job ID
     * @throws AuditException
     */
    @Override
    public String logJobStart(@Nonnull JobConfig config,
                              @Nonnull String correlationId,
                              @Nonnull JobExecutionContext context,
                              @Nonnull Class<? extends AbstractJob> type) throws AuditException {
        try {
            AbstractConnection<Session> connection = ConnectionManager.get().connection(connectionName, Session.class);
            if (!(connection instanceof HibernateConnection)) {
                throw new AuditException(String.format("Error getting DB connection. [name=%s]", connectionName));
            }

            JobAuditLog record = new JobAuditLog();
            record.setJobId(UUID.randomUUID().toString());
            record.setNamespace(config.getNamespace());
            record.setName(config.getName());
            record.setType(type.getCanonicalName());
            record.setStartTime(System.currentTimeMillis());
            record.setContextJson(GlobalConstants.getJsonMapper().writeValueAsString(config));
            record.setCorrelationId(correlationId);
            record.setJobState(EJobState.Running);

            Session session = connection.connection().getSession();
            try {
                Transaction tx = ((HibernateConnection) connection).startTransaction();
                try {
                    Object result = session.save(record);
                    if (result == null) {
                        throw new AuditException(String.format("Error creating entity. [type=%s]", record.getClass().getCanonicalName()));
                    }
                    tx.commit();
                } catch (Throwable t) {
                    tx.rollback();
                    throw t;
                }
                return record.getJobId();
            } finally {
                connection.close(session);
            }
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    /**
     * Log Job End
     *
     * @param id       - Unique Job ID
     * @param response - Processed response object.
     * @param error    - Execution error if job failed.
     * @throws AuditException
     */
    @Override
    public void logJobEnd(@Nonnull String id, Object response, Throwable error) throws AuditException {
        try {
            AbstractConnection<Session> connection = ConnectionManager.get().connection(connectionName, Session.class);
            if (!(connection instanceof HibernateConnection)) {
                throw new AuditException(String.format("Error getting DB connection. [name=%s]", connectionName));
            }

            Session session = connection.connection();
            try {
                JobAuditLog record = session.find(JobAuditLog.class, id);
                if (record == null) {
                    throw new AuditException(String.format("Audit Log record not found. [id=%s]", id));
                }
                record.setEndTime(System.currentTimeMillis());
                if (response != null) {
                    record.setResponseJson(GlobalConstants.getJsonMapper().writeValueAsString(response));
                }
                if (error != null) {
                    String err = error.getLocalizedMessage();
                    if (!Strings.isNullOrEmpty(err) && err.length() > MAX_ERROR_TEXT) {
                        err = err.substring(0, MAX_ERROR_TEXT);
                    }
                    record.setError(err);
                    String trace = LogUtils.getStackTrace(error);
                    record.setErrorTrace(trace);
                    record.setJobState(EJobState.Error);
                } else {
                    record.setJobState(EJobState.Finished);
                }

                Transaction tx = ((HibernateConnection) connection).startTransaction();
                try {
                    session.update(record);
                    tx.commit();
                } catch (Throwable t) {
                    tx.rollback();
                    throw t;
                }
            } finally {
                connection.close(session);
            }
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    /**
     * Update the Job Status.
     *
     * @param correlationId - Job Correlation ID
     * @param state         - Job State
     * @throws AuditException
     */
    @Override
    public void logJobState(@Nonnull String correlationId, EJobState state) throws AuditException {
        try {
            AbstractConnection<Session> connection = ConnectionManager.get().connection(connectionName, Session.class);
            if (!(connection instanceof HibernateConnection)) {
                throw new AuditException(String.format("Error getting DB connection. [name=%s]", connectionName));
            }

            Session session = connection.connection();
            try {
                String qstr = String.format("FROM %s WHERE correlationId = :correlation_id", JobAuditLog.class.getCanonicalName());
                Query query = session.createQuery(qstr);
                query.setParameter("correlation_id", correlationId);
                List<JobAuditLog> records = query.getResultList();
                if (records != null && !records.isEmpty()) {
                    JobAuditLog job = records.get(0);
                    job.setJobState(state);
                    job.setStateUpdateTimestamp(System.currentTimeMillis());
                    if (state == EJobState.Finished || state == EJobState.Stopped || state == EJobState.Error) {
                        job.setEndTime(System.currentTimeMillis());
                    }
                    Transaction tx = ((HibernateConnection) connection).startTransaction();
                    try {
                        session.update(job);
                        tx.commit();
                    } catch (Exception ex) {
                        LogUtils.error(getClass(), ex);
                        tx.rollback();
                        throw ex;
                    }
                } else {
                    LogUtils.error(getClass(), String.format("Job Not Found: [correlation ID=%s]", correlationId));
                }
            } finally {
                connection.close(session);
            }
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    /**
     * Update the Job with Error.
     *
     * @param correlationId - Job Correlation ID
     * @param error         - Error handle.
     * @throws AuditException
     */
    @Override
    public void logJobError(@Nonnull String correlationId, Throwable error) throws AuditException {
        try {
            AbstractConnection<Session> connection = ConnectionManager.get().connection(connectionName, Session.class);
            if (!(connection instanceof HibernateConnection)) {
                throw new AuditException(String.format("Error getting DB connection. [name=%s]", connectionName));
            }

            Session session = connection.connection();
            try {
                String qstr = String.format("FROM %s WHERE correlationId = :correlation_id", JobAuditLog.class.getCanonicalName());
                Query query = session.createQuery(qstr);
                query.setParameter("correlation_id", correlationId);
                List<JobAuditLog> records = query.getResultList();
                if (records != null && !records.isEmpty()) {
                    JobAuditLog job = records.get(0);
                    job.setJobState(EJobState.Error);
                    job.setEndTime(System.currentTimeMillis());
                    job.setStateUpdateTimestamp(System.currentTimeMillis());
                    if (error != null) {
                        job.setError(error.getLocalizedMessage());
                        String st = LogUtils.getStackTrace(error);
                        job.setErrorTrace(st);
                    } else {
                        job.setError("Job returned error state. [error=null]");
                    }
                    Transaction tx = ((HibernateConnection) connection).startTransaction();
                    try {
                        session.update(job);
                        tx.commit();
                    } catch (Exception ex) {
                        LogUtils.error(getClass(), ex);
                        tx.rollback();
                        throw ex;
                    }
                } else {
                    LogUtils.error(getClass(), String.format("Job Not Found: [correlation ID=%s]", correlationId));
                }
            } finally {
                connection.close(session);
            }
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    /**
     * Find List of Job ID(s) for jobs that are in pending status.
     *
     * @return - List of pending Job ID(s)
     * @throws AuditException
     */
    @Override
    public List<JobAuditLog> findPendingJobs() throws AuditException {
        try {
            AbstractConnection<Session> connection = ConnectionManager.get().connection(connectionName, Session.class);
            if (!(connection instanceof HibernateConnection)) {
                throw new AuditException(String.format("Error getting DB connection. [name=%s]", connectionName));
            }

            Session session = connection.connection();
            try {
                String qstr = String.format("FROM %s WHERE jobState = :job_state", JobAuditLog.class.getCanonicalName());
                Query query = session.createQuery(qstr);
                query.setParameter("job_state", EJobState.Pending);
                List<JobAuditLog> records = query.getResultList();
                if (records != null && !records.isEmpty()) {
                    return records;
                }
                return null;
            } finally {
                connection.close(session);
            }
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    /**
     * Find List of Job ID(s) for jobs that are in running status.
     *
     * @return - List of running Job ID(s)
     * @throws AuditException
     */
    @Override
    public List<JobAuditLog> findRunningJobs() throws AuditException {
        try {
            AbstractConnection<Session> connection = ConnectionManager.get().connection(connectionName, Session.class);
            if (!(connection instanceof HibernateConnection)) {
                throw new AuditException(String.format("Error getting DB connection. [name=%s]", connectionName));
            }

            Session session = connection.connection();
            try {
                String qstr = String.format("FROM %s WHERE jobState = :job_state", JobAuditLog.class.getCanonicalName());
                Query query = session.createQuery(qstr);
                query.setParameter("job_state", EJobState.Running);
                List<JobAuditLog> records = query.getResultList();
                if (records != null && !records.isEmpty()) {
                    return records;
                }
                return null;
            } finally {
                connection.close(session);
            }
        } catch (Throwable t) {
            throw new AuditException(t);
        }
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
        ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
    }

    @Override
    public void close() throws IOException {

    }
}
