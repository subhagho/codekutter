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

import com.codekutter.common.TimeWindow;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ICustomParser;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

public class TimeWindowParser implements ICustomParser<Integer> {
    /**
     * Parse the input configuration node to generate the return value.
     *
     * @param node - Configuration node.
     * @param name - Value name.
     * @return - Parsed value.
     * @throws ConfigurationException
     */
    @Override
    public Integer parse(@Nonnull AbstractConfigNode node, @Nonnull String name) throws ConfigurationException {
        node = node.find(name);
        try {
            if (node instanceof ConfigValueNode) {
                ConfigValueNode vn = (ConfigValueNode) node;
                String value = vn.getValue();
                if (!Strings.isNullOrEmpty(value)) {
                    TimeWindow window = TimeWindow.parse(value);
                    return (int) window.period();
                }
            }
            return null;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
