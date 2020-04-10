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

import com.codekutter.common.stores.DataStoreConfig;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.MappedSuperclass;
import javax.persistence.Table;

@Getter
@Setter
@MappedSuperclass
public class S3StoreConfig extends DataStoreConfig {
    public static final int DEFAULT_MAX_CACHE_SIZE = 128;
    public static final int DEFAULT_CACHE_EXPIRY = 5 * 60 * 1000;

    @Column(name = "bucket")
    @ConfigAttribute(required = true)
    private String bucket;
    @Column(name = "temp_directory")
    @ConfigValue
    private String tempDirectory;
    @Column(name = "user_cache")
    @ConfigAttribute
    private boolean useCache = true;
    @Column(name = "max_cache_size")
    @ConfigValue
    private int maxCacheSize = DEFAULT_MAX_CACHE_SIZE;
    @Column(name = "cache_expiry_window")
    @ConfigValue
    private long cacheExpiryWindow = DEFAULT_CACHE_EXPIRY;

    @Override
    public void postLoad() throws ConfigurationException {
        super.postLoad();
        if (useCache) {
            if (maxCacheSize <= 0) {
                maxCacheSize = DEFAULT_MAX_CACHE_SIZE;
            }
            if (cacheExpiryWindow <= 0) {
                cacheExpiryWindow = DEFAULT_CACHE_EXPIRY;
            }
        }
    }
}
