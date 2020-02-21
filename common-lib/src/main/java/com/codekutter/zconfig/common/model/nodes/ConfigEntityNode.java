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

package com.codekutter.zconfig.common.model.nodes;

import com.codekutter.zconfig.common.model.Configuration;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConfigEntityNode extends ConfigPathNode {
    private Class<?> entityType;
    private Object entity;

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigEntityNode() {
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigEntityNode(Configuration configuration, AbstractConfigNode parent) {
        super(configuration, parent);
    }
}
