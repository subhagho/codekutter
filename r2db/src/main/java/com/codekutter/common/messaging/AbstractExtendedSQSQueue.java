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

package com.codekutter.common.messaging;

import com.codekutter.common.GlobalConstants;
import com.codekutter.common.model.IKeyed;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.r2db.driver.impl.ExtendedSQSConnection;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.jms.Session;
import java.io.IOException;
import java.net.URLDecoder;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public abstract class AbstractExtendedSQSQueue<M extends IKeyed> extends AbstractJmsQueue<M> {

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
            queue(URLDecoder.decode(queue(), GlobalConstants.defaultCharset().name()));
            LogUtils.info(getClass(), String.format("Configuring Queue. [name=%s]...", name()));
            AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (!(cnode instanceof ConfigPathNode)) {
                throw new ConfigurationException(String.format("Invalid Queue configuration. [node=%s]", node.getAbsolutePath()));
            }
            AbstractConnection<Session> conn = ConnectionManager.get().readConnection((ConfigPathNode) cnode);
            if (!(conn instanceof ExtendedSQSConnection)) {
                throw new ConfigurationException(String.format("Invalid Extended SQS connection returned. [type=%s]", conn.getClass().getCanonicalName()));
            }
            connection(conn);
            session(connection().connection());

            setupMetrics(queue());
        } catch (Throwable t) {
            throw new ConfigurationException(t);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
