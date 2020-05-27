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

package com.codekutter.common.utils;

import com.codekutter.common.TimeWindow;
import com.codekutter.common.model.IKey;
import com.codekutter.common.model.IKeyed;
import com.codekutter.common.model.ObjectState;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.transformers.TimeWindowParser;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "multi-map-cache")
public abstract class AbstractMultiMapCache<K extends IKey, T extends IKeyed<K>> extends Runner implements IConfigurable, Closeable {
    @Setter(AccessLevel.NONE)
    protected final Class<? extends T> entityType;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    protected final ReentrantLock lock = new ReentrantLock();
    @ConfigAttribute(required = true)
    protected String name;
    @ConfigValue(name = "loader", required = true)
    protected String loaderClass;
    @Setter(AccessLevel.NONE)
    protected MultiMapCacheLoader<K, T> loader;
    @ConfigValue(required = true, parser = TimeWindowParser.class)
    protected TimeWindow refreshInterval;
    @Setter(AccessLevel.NONE)
    protected ObjectState state = new ObjectState();
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    protected ManagedThread loaderThread;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    protected long lastRunTime;

    public AbstractMultiMapCache(@Nonnull Class<? extends T> entityType) {
        this.entityType = entityType;
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
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            Class<? extends MultiMapCacheLoader> cls = (Class<? extends MultiMapCacheLoader>) Class.forName(loaderClass);
            loader = TypeUtils.createInstance(cls);
            AbstractConfigNode nn = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (nn == null) {
                throw new ConfigurationException(String.format("Type node not found. [type=%s][path=%s]", getClass().getCanonicalName(), node.getAbsolutePath()));
            }
            loader.configure(nn);
        } catch (Exception ex) {
            state.setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    public abstract Collection<T> get(@Nonnull K key);

    public abstract Set<K> keySet();

    public abstract Collection<T> values();

    public abstract boolean isEmpty();

    public abstract int size();
}
