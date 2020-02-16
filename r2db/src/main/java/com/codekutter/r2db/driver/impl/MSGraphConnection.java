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

package com.codekutter.r2db.driver.impl;

import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionException;
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.EncryptedValue;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.microsoft.graph.authentication.IAuthenticationProvider;
import com.microsoft.graph.core.DefaultClientConfig;
import com.microsoft.graph.core.IClientConfig;
import com.microsoft.graph.http.IHttpRequest;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
@Accessors(fluent = true)
public class MSGraphConnection extends AbstractConnection<IGraphServiceClient> {
    @ConfigValue(required = true)
    private String clientId;
    @ConfigValue(required = true)
    private String requestUrl;
    @ConfigValue(required = true)
    private String username;
    @ConfigValue(required = true)
    private EncryptedValue password;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    // Don't use password grant in your apps. Only use for legacy solutions and automated testing.
    private String grantType = "password";
    @Setter(AccessLevel.NONE)
    private String tokenEndpoint = "https://login.microsoftonline.com/common/oauth2/token";
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private String resourceId = "https%3A%2F%2Fgraph.microsoft.com%2F";
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private String accessToken = null;
    @Setter(AccessLevel.NONE)
    private IGraphServiceClient client;

    @Override
    public IGraphServiceClient connection() throws ConnectionException {
        state().checkOpened();
        return client;
    }

    @Override
    public boolean hasTransactionSupport() {
        return false;
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
            accessToken = GetAccessToken().replace("\"", "");
            IAuthenticationProvider mAuthenticationProvider = new IAuthenticationProvider() {
                @Override
                public void authenticateRequest(final IHttpRequest request) {
                    request.addHeader("Authorization",
                            "Bearer " + accessToken);
                }
            };
            IClientConfig mClientConfig = DefaultClientConfig.createWithAuthenticationProvider(mAuthenticationProvider);

            client = GraphServiceClient.fromConfig(mClientConfig);
            state().setState(EConnectionState.Open);
        } catch (Exception ex) {
            state().setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (state().getState() == EConnectionState.Open) {
            state().setState(EConnectionState.Closed);
        }
        if (client != null) {
            client = null;
        }
    }

    private String GetAccessToken() throws Exception {
        URL url = new URL(tokenEndpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String line;
        StringBuilder jsonString = new StringBuilder();

        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
        conn.setRequestMethod("POST");
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream(), StandardCharsets.UTF_8);
        String payload = String.format("grant_type=%1$s&resource=%2$s&client_id=%3$s&username=%4$s&password=%5$s",
                grantType,
                resourceId,
                clientId,
                username,
                password);
        writer.write(payload);
        writer.close();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            while ((line = br.readLine()) != null) {
                jsonString.append(line);
            }
            br.close();
        } catch (Exception e) {
            throw new Error("Error reading authorization response: " + e.getLocalizedMessage());
        }
        conn.disconnect();

        JsonObject res = new GsonBuilder().create().fromJson(jsonString.toString(), JsonObject.class);
        return res.get("access_token").toString().replaceAll("\"", "");
    }
}
