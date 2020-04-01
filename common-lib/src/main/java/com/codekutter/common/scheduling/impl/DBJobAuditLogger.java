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
import java.io.IOException;
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

            Session session = connection.connection().getSession();
            Transaction tx = session.beginTransaction();
            try {
                Object result = session.save(record);
                if (result == null) {
                    throw new AuditException(String.format("Error creating entity. [type=%s]", record.getClass().getCanonicalName()));
                }
                tx.commit();
            } catch (Throwable t) {
                tx.rollback();
                throw t;
            } finally {
                session.close();
            }
            return record.getJobId();
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

            Session session = connection.connection().getSession();
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
            }
            Transaction tx = session.beginTransaction();
            try {
                session.update(record);
                tx.commit();
            } catch (Throwable t) {
                tx.rollback();
            } finally {
                session.close();
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
