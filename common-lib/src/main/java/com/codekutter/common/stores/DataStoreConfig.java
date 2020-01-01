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

package com.codekutter.common.stores;

import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "dataStore")
public class DataStoreConfig {
    @ConfigAttribute(name = "dataStoreClass", required = true)
    @SuppressWarnings("rawtypes")
    private Class<? extends AbstractDataStore> dataStoreClass;
    @ConfigAttribute(name = "name", required = true)
    private String name;
    @ConfigValue(name = "description")
    private String description;
    @ConfigAttribute(name = "connection", required = true)
    private String connectionName;
}
