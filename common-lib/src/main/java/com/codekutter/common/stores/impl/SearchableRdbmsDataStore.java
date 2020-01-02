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

import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.DataStoreManager;
import com.codekutter.common.stores.ISearchable;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.lucene.search.Query;
import org.hibernate.Session;

import javax.annotation.Nonnull;
import java.util.List;

public class SearchableRdbmsDataStore extends RdbmsDataSource implements ISearchable {
    @Override
    public <T> List<T> textSearch(@Nonnull Query query, @Nonnull Class<? extends T> type) throws DataStoreException {
        return null;
    }

    @Override
    public <T> List<T> textSearch(@Nonnull Query query, int batchSize, int offset, @Nonnull Class<? extends T> type) throws DataStoreException {
        return null;
    }

    @Override
    public <T> List<T> textSearch(@Nonnull String query, @Nonnull Class<? extends T> type) throws DataStoreException {
        return null;
    }

    @Override
    public <T> List<T> textSearch(@Nonnull String query, int batchSize, int offset, @Nonnull Class<? extends T> type) throws DataStoreException {
        return null;
    }

    @Override
    public void configure(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        Preconditions.checkArgument(config() instanceof RdbmsConfig);
        AbstractConnection<Session> connection =
                dataStoreManager.getConnection(config().connectionName(), Session.class);
        if (!(connection instanceof HibernateConnection)) {
            throw new ConfigurationException(String.format("No connection found for name. [name=%s]", config().connectionName()));
        }
        withConnection(connection);
        HibernateConnection hibernateConnection = (HibernateConnection)connection;
        session = hibernateConnection.connection();
        HibernateConnection readConnection = null;
        if (!Strings.isNullOrEmpty(((RdbmsConfig)config()).readConnectionName())) {
            AbstractConnection<Session> rc =
                    (AbstractConnection<Session>) dataStoreManager.getConnection(((RdbmsConfig) config()).readConnectionName(), Session.class);
            if (!(rc instanceof HibernateConnection)) {
                throw new ConfigurationException(String.format("No connection found for name. [name=%s]", ((RdbmsConfig) config()).readConnectionName()));
            }
            readConnection = (HibernateConnection)rc;
        }
        if (readConnection != null) {
            readSession = readConnection.connection();
            readSession.setDefaultReadOnly(true);
        } else {
            readSession = session;
        }
    }
}
