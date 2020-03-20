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

import com.codekutter.common.utils.ICryptoHandler;
import com.codekutter.zconfig.common.BaseConfigEnv;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
@MappedSuperclass
public abstract class ConnectionConfig {
    @Id
    @Column(name = "name")
    private String name;
    @Column(name = "register_types")
    private String supportedTypesList;
    @Transient
    private Set<Class<?>> supportedTypes;

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
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
        postLoad();
    }

    public String decrypt(@Nonnull String encrypted) {
        if (!Strings.isNullOrEmpty(encrypted)) {
            try {
                ICryptoHandler handler = BaseConfigEnv.env().cryptoHandler();
                return handler.decryptAsString(encrypted, Charset.defaultCharset());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return encrypted;
    }

    public abstract void postLoad() throws ConfigurationException;
}
