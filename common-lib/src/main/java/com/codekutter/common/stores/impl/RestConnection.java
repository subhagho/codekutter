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

import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionException;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigAttributesNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
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
    private Client client;

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
        try {
            Map<String, String> config = configuration(node);
            ClientBuilder builder = ClientBuilder.newBuilder();
            if (config != null && !config.isEmpty()) {
                for (String key : config.keySet()) {
                    builder.property(key, config.get(key));
                }
            }
            if (useSSL) {
                builder.sslContext(SSLContext.getDefault());
            }
            client = builder.build();
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private Map<String, String> configuration(AbstractConfigNode node) throws ConfigurationException {
        if (node instanceof ConfigPathNode) {
            AbstractConfigNode cnode = node.find(CONFIG_PATH_CONFIG);
            if (cnode instanceof ConfigPathNode) {
                ConfigPathNode cp = (ConfigPathNode) cnode;
                if (cp.attributes() != null) {
                    ConfigAttributesNode attrs = cp.attributes();
                    Map<String, ConfigValueNode> map = attrs.getKeyValues();
                    if (map != null && !map.isEmpty()) {
                        Map<String, String> amap = new HashMap<>();
                        for (String key : map.keySet()) {
                            ConfigValueNode vn = map.get(key);
                            amap.put(key, vn.getValue());
                        }
                        return amap;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
