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

package com.codekutter.r2db.driver.impl;

import com.codekutter.common.model.ConnectionConfig;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Entity
@Table(name = "config_filesystem_aws")
public class AwsS3ConnectionConfig extends ConnectionConfig {
    public static final String DEFAULT_PROFILE = "default";

    @Column(name = "use_credentials")
    private boolean useCredentials = true;
    @Column(name = "region")
    private String region;
    @Column(name = "profile")
    private String profile = DEFAULT_PROFILE;
    @Column(name = "parameters")
    private String parameters;
    @Transient
    private Map<String, String> params = new HashMap<>();

    @Override
    public void postLoad() throws ConfigurationException {
        if (!Strings.isNullOrEmpty(parameters)) {
            String[] values = parameters.split(";");
            if (values.length > 0) {
                for (String value : values) {
                    if (!Strings.isNullOrEmpty(value)) {
                        String[] parts = value.split("=");
                        if (parts.length == 2) {
                            String key = parts[0];
                            String v = parts[1];
                            if (Strings.isNullOrEmpty(key)) {
                                LogUtils.warn(getClass(), String.format("Invalid parameter. [value=%s]", value));
                                continue;
                            }
                            params.put(key, v);
                        } else {
                            LogUtils.warn(getClass(), String.format("Invalid parameter. [value=%s]", value));
                        }
                    }
                }
            }
        }
    }
}
