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
import com.codekutter.common.scheduling.remote.EJobState;
import com.codekutter.common.scheduling.remote.JobResponse;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.stores.impl.RestConnection;
import com.codekutter.common.utils.LogUtils;
import com.google.common.base.Preconditions;
import org.apache.http.HttpStatus;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import javax.annotation.Nonnull;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

public class AsyncRESTCallJob extends AbstractJob {
    @Override
    public Object doExecute(@Nonnull String correlationId, @Nonnull JobExecutionContext context, @Nonnull JobConfig config) throws JobExecutionException {
        Preconditions.checkArgument(config instanceof AsyncRestJobConfig);
        Preconditions.checkArgument(config.isAsync());
        try {
            AsyncRestJobConfig rc = (AsyncRestJobConfig) config;
            RestConnection connection = (RestConnection) ConnectionManager.get().connection(rc.getConnectionName(), Client.class);
            if (connection == null) {
                throw new JobExecutionException(String.format("Connection not found. [type=%s][name=%s]",
                        RestConnection.class.getCanonicalName(), ((RestJobConfig) config).getConnectionName()));
            }
            WebTarget webTarget = connection.client().target(((RestJobConfig) config).getRequestUrl());
            switch (rc.getRequestType()) {
                case GET:
                    return doGet(correlationId, webTarget, rc);
                case PUT:
                    return doPut(correlationId, webTarget, rc);
                case POST:
                    return doPost(correlationId, webTarget, rc);
                case DELETE:
                    return doDelete(correlationId, webTarget, rc);
            }
            return null;
        } catch (Exception ex) {
            throw new JobExecutionException(ex);
        }
    }

    private Object doGet(String correlationId, WebTarget target, AsyncRestJobConfig config) throws JobExecutionException {
        Response response = null;
        if (config.getRequestBuilder() != null) {
            response = config.getRequestBuilder().get(correlationId, target, config);
        } else
            response = target.request(config.getMediaType()).get();
        if (response.getStatus() != HttpStatus.SC_OK) {
            throw new JobExecutionException(String.format("Request failed. [status=%d]", response.getStatus()));
        }
        if (config.getResponseHandler() != null) {
            return config.getResponseHandler().handle(response);
        }
        return processResponse(correlationId, response);
    }

    private Object doPost(String correlationId, WebTarget target, AsyncRestJobConfig config) throws JobExecutionException {
        Response response = null;
        if (config.getRequestBuilder() != null) {
            response = config.getRequestBuilder().post(correlationId, target, config);
        } else
            throw new JobExecutionException("Request Builder not specified. Required for POST requests.");
        if (response.getStatus() != HttpStatus.SC_OK) {
            throw new JobExecutionException(String.format("Request failed. [status=%d]", response.getStatus()));
        }
        if (config.getResponseHandler() != null) {
            return config.getResponseHandler().handle(response);
        }
        return processResponse(correlationId, response);
    }

    private Object doPut(String correlationId, WebTarget target, AsyncRestJobConfig config) throws JobExecutionException {
        Response response = null;
        if (config.getRequestBuilder() != null) {
            response = config.getRequestBuilder().put(correlationId, target, config);
        } else
            throw new JobExecutionException("Request Builder not specified. Required for PUT requests.");
        if (response.getStatus() != HttpStatus.SC_OK) {
            throw new JobExecutionException(String.format("Request failed. [status=%d]", response.getStatus()));
        }
        if (config.getResponseHandler() != null) {
            return config.getResponseHandler().handle(response);
        }
        return processResponse(correlationId, response);
    }

    private Object doDelete(String correlationId, WebTarget target, AsyncRestJobConfig config) throws JobExecutionException {
        Response response = null;
        if (config.getRequestBuilder() != null) {
            response = config.getRequestBuilder().delete(correlationId, target, config);
        } else
            throw new JobExecutionException("Request Builder not specified. Required for DELETE requests.");
        if (response.getStatus() != HttpStatus.SC_OK) {
            throw new JobExecutionException(String.format("Request failed. [status=%d]", response.getStatus()));
        }
        if (config.getResponseHandler() != null) {
            return config.getResponseHandler().handle(response);
        }
        return processResponse(correlationId, response);
    }

    private JobResponse processResponse(String correlationId, Response response) throws JobExecutionException {
        JobResponse jr = response.readEntity(JobResponse.class);
        if (jr.getJobState().hasError()) {
            throw new JobExecutionException(jr.getJobState().getError());
        } else {
            EJobState state = EJobState.Error;
            if (jr.getJobState() != null) {
                state = jr.getJobState().getState();
            } else {
                throw new JobExecutionException("NULL Job State returned...");
            }
            auditJobState(correlationId, state);
        }
        LogUtils.debug(getClass(), jr);
        return jr;
    }
}
