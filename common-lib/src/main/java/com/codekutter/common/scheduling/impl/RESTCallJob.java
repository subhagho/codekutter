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
import org.apache.http.HttpStatus;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import javax.annotation.Nonnull;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

public class RESTCallJob extends AbstractJob {
    @Override
    public Object doExecute(@Nonnull String correlationId,
                            @Nonnull JobExecutionContext context,
                            @Nonnull JobConfig config) throws JobExecutionException {
        Preconditions.checkArgument(config instanceof RestJobConfig);
        try {
            RestJobConfig rc = (RestJobConfig) config;
            RestConnection connection = (RestConnection) ConnectionManager.get().connection(rc.getConnectionName(), Client.class);
            if (connection == null) {
                throw new JobExecutionException(String.format("Connection not found. [type=%s][name=%s]",
                        RestConnection.class.getCanonicalName(), ((RestJobConfig) config).getConnectionName()));
            }
            WebTarget webTarget = connection.client().target(((RestJobConfig) config).getRequestUrl());
            switch (((RestJobConfig) config).getRequestType()) {
                case GET:
                    return doGet(correlationId, webTarget, (RestJobConfig) config);
                case PUT:
                    return doPut(correlationId, webTarget, (RestJobConfig) config);
                case POST:
                    return doPost(correlationId, webTarget, (RestJobConfig) config);
                case DELETE:
                    return doDelete(correlationId, webTarget, (RestJobConfig) config);
            }
            return null;
        } catch (Exception ex) {
            throw new JobExecutionException(ex);
        }
    }

    private Object doGet(String correlationId, WebTarget target, RestJobConfig config) throws JobExecutionException {
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
        return response.readEntity(String.class);
    }

    private Object doPost(String correlationId, WebTarget target, RestJobConfig config) throws JobExecutionException {
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
        return response.readEntity(String.class);
    }

    private Object doPut(String correlationId, WebTarget target, RestJobConfig config) throws JobExecutionException {
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
        return response.readEntity(String.class);
    }

    private Object doDelete(String correlationId, WebTarget target, RestJobConfig config) throws JobExecutionException {
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
        return response.readEntity(String.class);
    }
}
