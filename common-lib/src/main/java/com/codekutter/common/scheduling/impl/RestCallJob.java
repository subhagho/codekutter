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

import com.codekutter.common.scheduling.AbstractJob;
import com.codekutter.common.scheduling.JobConfig;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.stores.impl.RestConnection;
import com.google.common.base.Preconditions;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import javax.annotation.Nonnull;
import javax.ws.rs.client.Client;

public class RestCallJob extends AbstractJob {
    @Override
    public void doExecute(@Nonnull JobExecutionContext context, @Nonnull JobConfig config) throws JobExecutionException {
        Preconditions.checkArgument(config instanceof RestJobConfig);
        try {
            RestJobConfig rc = (RestJobConfig)config;
            RestConnection connection = (RestConnection) ConnectionManager.get().connection(rc.getConnectionName(), Client.class);

        } catch (Exception ex) {
            throw new JobExecutionException(ex);
        }
    }
}
