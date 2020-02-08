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

import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.transformers.TimeWindowParser;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class JobConfig implements IConfigurable {
    public static final String CONTEXT_PARAM_CONFIG = "jobs.context.config";

    @ConfigAttribute(required = true)
    private String name;
    @ConfigAttribute(required = true)
    private String namespace;
    @ConfigValue(required = true, parser = TimeWindowParser.class)
    private int scheduleInterval;
    @ConfigValue
    private int priority = 999;
    @ConfigAttribute(required = true)
    private Class<? extends AbstractJob> type;

    public String jobKey() {
        return key(namespace, name);
    }

    public static String key(@Nonnull String namespace, @Nonnull String name) {
        return String.format("%s.%s", namespace, name);
    }
}
