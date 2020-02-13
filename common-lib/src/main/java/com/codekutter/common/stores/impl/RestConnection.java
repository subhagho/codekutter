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

import com.codekutter.common.ValueParseException;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionException;
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigAttributesNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class RestConnection extends AbstractConnection<Client> {
    public static final String CONFIG_PATH_CONFIG = "configuration";

    @ConfigAttribute
    private boolean useSSL = false;
    @Setter(AccessLevel.NONE)
    protected Client client;

    @Override
    public Client connection() throws ConnectionException {
        if (client == null) {
            throw new ConnectionException("No client configured.", getClass());
        }
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
            Map<String, Object> config = configuration(node);
            ClientBuilder builder = ClientBuilder.newBuilder();
            if (config != null && !config.isEmpty()) {
                for (String key : config.keySet()) {
                    builder.property(key, config.get(key));
                }
            }
            if (useSSL) {
                builder.sslContext(SSLContext.getDefault());
            }
            client = builder.register(JacksonJaxbJsonProvider.class).build();
            state().setState(EConnectionState.Open);
        } catch (Exception ex) {
            state().setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    protected Map<String, Object> configuration(AbstractConfigNode node) throws ConfigurationException, ValueParseException {
        if (node instanceof ConfigPathNode) {
            AbstractConfigNode cnode = node.find(CONFIG_PATH_CONFIG);
            if (cnode instanceof ConfigPathNode) {
                ConfigPathNode cp = (ConfigPathNode) cnode;
                if (cp.attributes() != null) {
                    ConfigAttributesNode attrs = cp.attributes();
                    Map<String, ConfigValueNode> map = attrs.getKeyValues();
                    if (map != null && !map.isEmpty()) {
                        Map<String, Object> amap = new HashMap<>();
                        for (String key : map.keySet()) {
                            ConfigValueNode vn = map.get(key);
                            amap.put(key, vn.getParsedValue());
                        }
                        return amap;
                    }
                }
            }
        }
        return null;
    }

    public Invocation.Builder target(@Nonnull String url) throws ConnectionException{
        state().checkOpened();
        return client.target(url).request();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
