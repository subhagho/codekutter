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

import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListElementNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "queues")
@SuppressWarnings("rawtypes")
public class QueueManager implements IConfigurable, Closeable {
    @Setter(AccessLevel.NONE)
    private QueueCachingConfig cachingConfig;

    @Setter(AccessLevel.NONE)
    private Map<String, AbstractQueue> queues = new HashMap<>();

    @SuppressWarnings("unchecked")
    public <C, M> AbstractQueue<C, M> getQueue(@Nonnull String name,
                                               @Nonnull Class<? extends C> connectionType,
                                               @Nonnull Class<? extends M> messageType) {
        return queues.get(name);
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    @SuppressWarnings("rawtypes")
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        LogUtils.info(getClass(), "Initializing Queue Manager...");
        AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
        if (cnode instanceof ConfigPathNode) {
            cnode = ConfigUtils.getPathNode(AbstractQueue.class, (ConfigPathNode) cnode);
            if (cnode instanceof ConfigPathNode) {
                AbstractQueue queue = readQueue((ConfigPathNode) cnode);
                queues.put(queue.name(), queue);
            }
        } else if (cnode instanceof ConfigListElementNode) {
            List<ConfigElementNode> nodes = ((ConfigListElementNode) cnode).getValues();
            if (nodes != null && !nodes.isEmpty()) {
                for (ConfigElementNode n : nodes) {
                    AbstractQueue queue = readQueue((ConfigPathNode) n);
                    queues.put(queue.name(), queue);
                }
            }
        }
        AbstractConfigNode qcnode = ConfigUtils.getPathNode(QueueCachingConfig.class, (ConfigPathNode) node);
        if (qcnode instanceof ConfigPathNode) {
            cachingConfig = new QueueCachingConfig();
            cachingConfig = ConfigurationAnnotationProcessor.readConfigAnnotations(cachingConfig.getClass(), (ConfigPathNode) qcnode, cachingConfig);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <C, M> AbstractQueue<C, M> readQueue(@Nonnull ConfigPathNode node) throws ConfigurationException {
        try {
            String cname = ConfigUtils.getClassAttribute(node);
            if (Strings.isNullOrEmpty(cname)) {
                throw new ConfigurationException(String.format("Invalid queue configuration: Missing class type. [node=%s]", node.getAbsolutePath()));
            }
            Class<? extends AbstractQueue> cls = (Class<? extends AbstractQueue>) Class.forName(cname);
            AbstractQueue<?, ?> queue = cls.newInstance();
            queue.configure(node);

            LogUtils.debug(getClass(), String.format("Initialized Queue : [type=%s][name=%s]", queue.getClass().getCanonicalName(), queue.name()));
            return (AbstractQueue<C, M>) queue;
        } catch (Throwable t) {
            throw new ConfigurationException(t);
        }
    }

    @Override
    public void close() throws IOException {
        LogUtils.warn(getClass(), "Disposing Queue Manager...");
        if (!queues.isEmpty()) {
            for (String name : queues.keySet()) {
                AbstractQueue queue = queues.get(name);
                queue.close();
            }
            queues.clear();
        }
    }

    public static final QueueManager __instance = new QueueManager();

    public static void setup(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        __instance.configure(node);
    }

    public static QueueManager get() {
        return __instance;
    }
}
