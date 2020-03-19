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

package com.codekutter.zconfig.common.transformers;

import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ICustomParser;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListValueNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuppressWarnings("rawtypes")
public class ClassSetParser implements ICustomParser<Set<Class>> {
    /**
     * Parse the input configuration node to generate the return value.
     *
     * @param node - Configuration node.
     * @param name - Value name.
     * @return - Parsed value.
     * @throws ConfigurationException
     */
    @Override
    public Set<Class> parse(@Nonnull AbstractConfigNode node, @Nonnull String name) throws ConfigurationException {
        try {
            node = node.find(name);
            Set<Class> values = null;
            if (node instanceof ConfigPathNode) {
                AbstractConfigNode cnode = null;
                for (String key : ((ConfigPathNode) node).getChildren().keySet()) {
                    cnode = ((ConfigPathNode) node).getChildren().get(key);
                    break;
                }
                if (cnode instanceof ConfigValueNode) {
                    values = new HashSet<>();
                    Class cls = Class.forName(((ConfigValueNode) cnode).getValue());
                    values.add(cls);
                }
            } else if (node instanceof ConfigListValueNode) {
                List<ConfigValueNode> nodes = ((ConfigListValueNode) node).getValues();
                if (nodes != null && !nodes.isEmpty()) {
                    values = new HashSet<>();
                    for (ConfigValueNode vn : nodes) {
                        Class cls = Class.forName(vn.getValue());
                        values.add(cls);
                    }
                }
            }
            return values;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
