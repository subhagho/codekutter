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

package com.codekutter.r2db.driver.azure;

import com.codekutter.common.StateException;
import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.ObjectState;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.PublicClientApplication;
import com.microsoft.aad.msal4j.UserNamePasswordParameters;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Future;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "azure-auth")
public class AzureAuthManager implements IConfigurable, Closeable {
    @ConfigAttribute(name = "id", required = true)
    private String applicationId;
    @ConfigValue(name = "authority", required = true)
    private String authorityUrl;

    @Setter(AccessLevel.NONE)
    private ObjectState state = new ObjectState();

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

            LogUtils.debug(getClass(), String.format("Initialized Azure authentication. [authority=%s]", authorityUrl));
            state.setState(EObjectState.Available);
        } catch (Exception ex) {
            state.setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    private IAuthenticationResult getAccessToken(String userName, String password, String permission) throws Exception {

        PublicClientApplication app =
                PublicClientApplication
                        .builder(applicationId)
                        .authority(authorityUrl)
                        .build();

        Set<String> scopes = Collections.singleton(permission);
        UserNamePasswordParameters parameters =
                UserNamePasswordParameters
                        .builder(scopes, userName, password.toCharArray())
                        .build();

        Future<IAuthenticationResult> result = app.acquireToken(parameters);
        return result.get();
    }

    public String getUserInfoFromGraph(String accessToken) throws IOException{
        URL url = new URL("https://graph.microsoft.com/v1.0/me");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Bearer " + accessToken);
        conn.setRequestProperty("Accept", "application/json");

        int httpResponseCode = conn.getResponseCode();
        if(httpResponseCode == 200) {

            StringBuilder response;
            try(BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))){
                String inputLine;
                response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
            }
            return response.toString();
        } else {
            return String.format("Connection returned HTTP code: %s with message: %s",
                    httpResponseCode, conn.getResponseMessage());
        }
    }

    private static final AzureAuthManager __instance = new AzureAuthManager();

    private static AzureAuthManager get() throws StateException {
        __instance.state.check(EObjectState.Available, AzureAuthManager.class);
        return __instance;
    }

    public static void setup(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        __instance.configure(node);
    }

    @Override
    public void close() throws IOException {
        if (state.getState() == EObjectState.Available) {
            state.setState(EObjectState.Disposed);
        }
    }
}
