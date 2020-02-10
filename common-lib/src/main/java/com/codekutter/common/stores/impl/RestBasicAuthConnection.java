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

package com.codekutter.common.stores.impl;

import com.codekutter.common.stores.ConnectionException;
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.EncryptedValue;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class RestBasicAuthConnection extends RestConnection {
    @ConfigValue(required = true)
    private String username;
    @ConfigValue(required = true)
    private EncryptedValue password;

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
            Map<String, Object> config = configuration(node);
            ClientBuilder builder = ClientBuilder.newBuilder();
            if (config != null && !config.isEmpty()) {
                for (String key : config.keySet()) {
                    builder.property(key, config.get(key));
                }
            }
            if (useSSL()) {
                builder.sslContext(SSLContext.getDefault());
            }
            HttpAuthenticationFeature feature = HttpAuthenticationFeature.basicBuilder().build();
            client = builder.register(JacksonJaxbJsonProvider.class).register(feature).build();
            state().setState(EConnectionState.Open);
        } catch (Exception ex) {
            state().setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public Invocation.Builder target(@Nonnull String url) throws ConnectionException {
       state().checkOpened();
       return client.target(url).request()
               .property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_USERNAME, username)
               .property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_PASSWORD, password.getDecryptedValue());
    }
}
