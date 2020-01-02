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

package com.codekutter.common.stores.impl;

import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.EncryptedValue;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.Session;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class SearchableHibernateConnection extends HibernateConnection {
    public static final String CONFIG_INDEX_MANAGER = "hibernate.search.default.indexmanager";
    public static final String CONFIG_INDEX_MANAGER_VALUE = "elasticsearch";
    public static final String CONFIG_INDEX_MANAGER_HOSTS = "hibernate.search.default.elasticsearch.host";
    public static final String CONFIG_INDEX_MANAGER_USER = "hibernate.search.default.elasticsearch.username";
    public static final String CONFIG_INDEX_MANAGER_PASSWD = "hibernate.search.default.elasticsearch.password";

    @ConfigValue(name = "hosts")
    private List<String> elasticSearchHosts;
    @ConfigValue(name = "elasticSearchUsername")
    private String elasticSearchUsername;
    @ConfigValue(name = "elasticSearchPassword")
    private EncryptedValue elasticSearchPassword;

    @Override
    public Session connection() {
        return super.connection();
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        super.configure(node);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
