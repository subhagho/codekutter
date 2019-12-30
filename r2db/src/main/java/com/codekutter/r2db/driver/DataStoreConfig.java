package com.codekutter.r2db.driver;

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
