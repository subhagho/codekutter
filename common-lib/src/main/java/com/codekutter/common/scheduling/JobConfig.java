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

package com.codekutter.common.scheduling;

import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.transformers.TimeWindowParser;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;

@Getter
@Setter
@ConfigPath(path = "job")
public class JobConfig implements IConfigurable {
    public static final String CONTEXT_PARAM_CONFIG = "jobs.context.config";

    @ConfigAttribute(required = true)
    private String name;
    @ConfigAttribute(required = true)
    private String namespace;
    @ConfigValue(required = true, parser = TimeWindowParser.class)
    private int scheduleInterval;
    @ConfigValue
    private int priority = 999;
    @ConfigValue(required = true)
    private Class<? extends AbstractJob> type;
    @ConfigValue
    private boolean audited = false;
    @Setter(AccessLevel.NONE)
    @JsonIgnore
    private ScheduleManager manager;

    public JobConfig withScheduleManager(@Nonnull ScheduleManager manager) {
        this.manager = manager;
        return this;
    }

    public String jobKey() {
        return key(namespace, name);
    }

    public static String key(@Nonnull String namespace, @Nonnull String name) {
        return String.format("%s.%s", namespace, name);
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
        ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
    }
}
