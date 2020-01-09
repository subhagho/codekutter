package com.codekutter.r2db.driver.impl;

import com.codekutter.common.stores.DataStoreConfig;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class S3StoreConfig extends DataStoreConfig {
    @ConfigAttribute(required = true)
    private String bucket;
    @ConfigValue
    private String tempDirectory;
}
