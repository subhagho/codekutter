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
import com.codekutter.zconfig.common.IConfigurable;
import org.quartz.JobExecutionContext;

import javax.annotation.Nonnull;
import java.io.Closeable;

/**
 * Interface for defining Scheduled Job audit logger.
 */
public interface IJobAuditLogger extends IConfigurable, Closeable {
    /**
     * Log Job start.
     *
     * @param config  - Job Config
     * @param context - Execution Context
     * @param type    - Job Type
     * @return - Unique Job ID
     * @throws AuditException
     */
    String logJobStart(@Nonnull JobConfig config,
                       @Nonnull JobExecutionContext context,
                       @Nonnull Class<? extends AbstractJob> type) throws AuditException;

    /**
     * Log Job End
     *
     * @param id       - Unique Job ID
     * @param response - Processed response object.
     * @param error    - Execution error if job failed.
     * @throws AuditException
     */
    void logJobEnd(@Nonnull String id, Object response, Throwable error) throws AuditException;
}
