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

import com.codekutter.common.stores.ConnectionException;
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.transformers.StringListParser;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class ElasticSearchConnection extends SearchableConnection<RestHighLevelClient> {
    @ConfigValue(name = "hosts", required = true, parser = StringListParser.class)
    private List<String> hosts;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private RestHighLevelClient client = null;

    @Override
    public RestHighLevelClient connection() {
        Preconditions.checkState(state().isOpen());
        return client;
    }

    public RestClient restClient() {
        Preconditions.checkState(state().isOpen());
        return client.getLowLevelClient();
    }

    @Override
    public boolean hasTransactionSupport() {
        return false;
    }

    @Override
    public void close(@Nonnull RestHighLevelClient connection) throws ConnectionException {
        try {
            connection.close();
        } catch (IOException e) {
            throw new ConnectionException(e, getClass());
        }
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            HttpHost[] array = new HttpHost[hosts.size()];
            int indx = 0;
            for (String host : hosts) {
                String[] parts = host.split(":");
                if (parts.length < 2) {
                    throw new ConfigurationException(String.format("Invalid host definition. [host=%s]", host));
                }
                int port = Integer.parseInt(parts[1]);
                HttpHost h = new HttpHost(parts[0], port, "http");
                array[indx] = h;
                indx++;
            }
            client = new RestHighLevelClient(RestClient.builder(array));

            state().setState(EConnectionState.Open);
        } catch (Throwable t) {
            state().setError(t);
            throw new ConfigurationException(t);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null && state().isOpen()) {
            state().setState(EConnectionState.Closed);
            client = null;
        }
    }
}
