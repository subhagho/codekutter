package com.codekutter.r2db.driver.impl;

import com.codekutter.common.stores.DataStoreConfig;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class LocalDirStoreConfig extends DataStoreConfig {
    @ConfigValue(required = true)
    private String directory;
}
