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
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.EncryptedValue;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class ZookeeperConnection extends AbstractConnection<CuratorFramework> {
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final int DEFAULT_RETRY_INTERVAL = 300;

    @ConfigValue(name = "host", required = true)
    private String zkHost;
    @ConfigValue(name = "port", required = true)
    private int zkPort;
    @ConfigValue(name = "username", required = true)
    private String username;
    @ConfigValue(name = "password", required = true)
    private EncryptedValue password;
    @ConfigValue(name = "maxRetries")
    private int maxRetries = DEFAULT_MAX_RETRIES;
    @ConfigValue(name = "retryInterval")
    private int retryInterval = DEFAULT_RETRY_INTERVAL;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private CuratorFramework client = null;

    @Override
    public CuratorFramework connection() {
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
            RetryPolicy policy = new RetryNTimes(maxRetries, retryInterval);
            String conn = String.format("%s:%d", zkHost, zkPort);
            String auth = String.format("%s:%s", username, password.getDecryptedValue());
            client = CuratorFrameworkFactory.builder().connectString(conn).retryPolicy(policy).authorization("digest", auth.getBytes(StandardCharsets.UTF_8)).aclProvider(new ACLProvider() {
                @Override
                public List<ACL> getDefaultAcl() {
                    return ZooDefs.Ids.CREATOR_ALL_ACL;
                }

                @Override
                public List<ACL> getAclForPath(String s) {
                    return ZooDefs.Ids.CREATOR_ALL_ACL;
                }
            }).build();
            client.start();
            state().setState(EConnectionState.Open);
            LogUtils.debug(getClass(), String.format("Opened connection. [type=%s][name=%s]", getClass().getCanonicalName(), name()));
        } catch (Throwable t) {
            state().setError(t);
            throw new ConfigurationException(t);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
        if (state().isOpen()) {
            state().setState(EConnectionState.Closed);
        }
    }
}
