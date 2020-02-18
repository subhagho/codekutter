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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.codekutter.common.messaging.AwsSQSConnection;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionException;
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigParametersNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class AwsS3Connection extends AbstractConnection<AmazonS3> {
    public static final String DEFAULT_PROFILE = "profile";

    @ConfigAttribute(required = true)
    private String region;
    @ConfigAttribute
    private String profile = DEFAULT_PROFILE;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private AmazonS3 client = null;

    @Override
    public AmazonS3 connection() throws ConnectionException {
        try {
            state().checkOpened();
            return client;
        } catch (Throwable t) {
            throw new ConnectionException(t, getClass());
        }
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
            AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (!(cnode instanceof ConfigPathNode)) {
                throw new ConfigurationException(String.format("Invalid connection configuration. [node=%s]", node.getAbsolutePath()));
            }
            ClientConfiguration config = configBuilder((ConfigPathNode) cnode);
            ProfileCredentialsProvider provider = new ProfileCredentialsProvider(profile);
            // Only to check a valid profile is specified.
            provider.getCredentials();
            client = AmazonS3ClientBuilder.standard()
                    .withRegion(region)
                    .withClientConfiguration(config)
                    .withCredentials(provider).build();
            state().setState(EConnectionState.Open);
        } catch (Throwable t) {
            state().setError(t);
            throw new ConfigurationException(t);
        }
    }

    private ClientConfiguration configBuilder(ConfigPathNode node) throws ConfigurationException {
        ClientConfiguration config = new ClientConfiguration();
        ConfigParametersNode params = node.parmeters();
        if (params != null && !params.getKeyValues().isEmpty()) {
            Map<String, ConfigValueNode> values = params.getKeyValues();
            for (String f : values.keySet()) {
                boolean ret = ReflectionUtils.setValueFromString(values.get(f).getValue(), config, ClientConfiguration.class, f);
                if (!ret) {
                    LogUtils.warn(AwsSQSConnection.class, String.format("Ignored Invalid configuration : [property=%s]", f));
                } else {
                    LogUtils.debug(AwsSQSConnection.class, String.format("Set client configuration [property=%s]", f));
                }
            }
        }
        return config;
    }

    @Override
    public void close() throws IOException {
        if (state().isOpen()) {
            state().setState(EConnectionState.Closed);
        }
        client = null;
    }
}
