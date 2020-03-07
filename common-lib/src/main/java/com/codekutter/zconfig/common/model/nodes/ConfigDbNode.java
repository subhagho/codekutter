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

import com.codekutter.zconfig.common.model.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ConfigDbNode extends ConfigPathNode {
    @Setter(AccessLevel.NONE)
    private ConfigDbRecord record;

    /**
     * Default constructor - Initialize the state object.
     */
    public ConfigDbNode() {
    }

    /**
     * Constructor with Configuration and Parent node.
     *
     * @param configuration - Configuration this node belong to.
     * @param parent        - Parent node.
     */
    public ConfigDbNode(Configuration configuration, AbstractConfigNode parent) {
        super(configuration, parent);
    }


    public List<ConfigDbRecord> getChildRecord() {
        List<ConfigDbRecord> records = new ArrayList<>();
        ConfigurationSettings settings = getConfiguration().getSettings();

        for (String nn : getChildren().keySet()) {
            AbstractConfigNode node = getChildNode(nn);
            if (node instanceof ConfigValueNode) {
                records.add(getValueRecord((ConfigValueNode) node, -1));
            } else if (node instanceof ConfigListValueNode) {
                ConfigListValueNode listValueNode = (ConfigListValueNode) node;
                List<ConfigValueNode> values = listValueNode.getValues();
                int index = 0;
                for (ConfigValueNode vn : values) {
                    records.add(getValueRecord(vn, index));
                    index++;
                }
            } else if (node instanceof ConfigDbNode) {
                List<ConfigDbRecord> rs = ((ConfigDbNode) node).getChildRecord();
                if (rs != null && !rs.isEmpty()) {
                    for (ConfigDbRecord record : rs) {
                        record.getId().setPath(getAbsolutePath());
                        record.getId().setName(String.format("%s.%s", node.getName(), record.getId().getName()));
                        records.add(record);
                    }
                }
            }
            if (properties() != null && !properties().isEmpty()) {
                Map<String, ConfigValueNode> props = properties().getKeyValues();
                for(String key : props.keySet()) {
                    ConfigValueNode vn = props.get(key);
                    records.add(getValueRecord(vn, -1));
                }
            }
            if (parmeters() != null && !parmeters().isEmpty()) {
                Map<String, ConfigValueNode> props = parmeters().getKeyValues();
                for(String key : props.keySet()) {
                    ConfigValueNode vn = props.get(key);
                    records.add(getValueRecord(vn, -1));
                }
            }
            if (attributes() != null && !attributes().isEmpty()) {
                Map<String, ConfigValueNode> props = attributes().getKeyValues();
                for(String key : props.keySet()) {
                    ConfigValueNode vn = props.get(key);
                    records.add(getValueRecord(vn, -1));
                }
            }
        }
        if (!records.isEmpty()) return records;
        return null;
    }

    private ConfigDbRecord getValueRecord(ConfigValueNode node, int index) {
        ConfigDbRecordId id = new ConfigDbRecordId();
        id.setConfigId(getConfiguration().getId());
        id.setMajorVersion(getConfiguration().getVersion().getMajorVersion());
        if (index < 0)
            id.setName(node.getDbNodeName());
        else
            id.setName(String.format("%s/%d", node.getName(), index));

        id.setPath(node.getParent().getAbsolutePath());
        ConfigDbRecord record = new ConfigDbRecord();
        record.setId(id);
        if (node instanceof EncryptedValue) {
            EncryptedValue ev = (EncryptedValue) node;
            record.setEncrypted(true);
        }
        record.setValue(((ConfigValueNode) node).getValue());
        record.setValueType(((ConfigValueNode) node).getValueType());

        return record;
    }

}
