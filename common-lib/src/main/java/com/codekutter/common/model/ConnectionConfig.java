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

package com.codekutter.common.model;

import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;
import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
@MappedSuperclass
@SuppressWarnings("rawtypes")
public abstract class ConnectionConfig {
    @Id
    @Column(name = "name")
    private String name;
    @Column(name = "connection_type")
    private String connectionTypeString;
    @Transient
    private Class<? extends AbstractConnection> connectionType;
    @Column(name = "register_types")
    private String supportedTypesList;
    @Transient
    private Set<Class<?>> supportedTypes;

    @SuppressWarnings("unchecked")
    public void load() throws ConfigurationException {
        try {
            if (!Strings.isNullOrEmpty(supportedTypesList)) {
                String[] types = supportedTypesList.split(";");
                if (types.length > 0) {
                    supportedTypes = new HashSet<>();
                    for (String type : types) {
                        if (Strings.isNullOrEmpty(type)) continue;
                        Class<?> cls = Class.forName(type);
                        supportedTypes.add(cls);
                    }
                }
            }
            if (Strings.isNullOrEmpty(connectionTypeString)) {
                throw ConfigurationException.propertyNotFoundException("connectionTypeString");
            }
            connectionType = (Class<? extends AbstractConnection>) Class.forName(connectionTypeString);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
        postLoad();
    }

    public abstract void postLoad() throws ConfigurationException;
}
