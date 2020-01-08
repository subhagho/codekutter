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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.codekutter.common.Context;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.AbstractDirectoryStore;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.DataStoreManager;
import com.codekutter.r2db.driver.model.S3FileEntity;
import com.codekutter.r2db.driver.model.S3FileKey;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class AwsS3DataStore extends AbstractDirectoryStore<AmazonS3> {
    @ConfigAttribute(required = true)
    private String bucket;
    private AwsS3Connection connection = null;

    @Override
    public <S, T> void move(S source, T target, Context context) throws DataStoreException {
        Preconditions.checkArgument(source instanceof S3FileKey);
        Preconditions.checkArgument(target instanceof S3FileKey);
        Preconditions.checkArgument(connection != null && connection.state().isOpen());

        S3FileKey sk = (S3FileKey)source;
        S3FileKey tk = (S3FileKey)target;
        try {
            CopyObjectRequest copyObjRequest = new CopyObjectRequest(sk.bucket(), sk.key(), tk.bucket(), tk.key());
            connection.connection().copyObject(copyObjRequest);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void configure(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {

    }

    @Override
    public <E extends IEntity> E create(@Nonnull E entity, @Nonnull Class<? extends IEntity> type, Context context) throws DataStoreException {
        Preconditions.checkArgument(entity instanceof S3FileEntity);
        Preconditions.checkArgument(connection != null && connection.state().isOpen());

        try {
            String key = ((S3FileEntity)entity).withClient(connection.connection()).copyToRemote();
            if (Strings.isNullOrEmpty(key)) {
                throw new DataStoreException(String.format("Error uploading file to S3. [key=%s]", ((S3FileEntity)entity.getKey())));
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
        return entity;
    }

    @Override
    public <E extends IEntity> E update(@Nonnull E entity, @Nonnull Class<? extends IEntity> type, Context context) throws DataStoreException {
        Preconditions.checkArgument(entity instanceof S3FileEntity);
        Preconditions.checkArgument(connection != null && connection.state().isOpen());

        try {
            if (!((S3FileEntity)entity).remoteExists()) {
                throw new DataStoreException(String.format("Specified file doesn't exist. [key=%s]", entity.getKey().toString()));
            }
            String key = ((S3FileEntity)entity).withClient(connection.connection()).copyToRemote();
            if (Strings.isNullOrEmpty(key)) {
                throw new DataStoreException(String.format("Error uploading file to S3. [key=%s]", ((S3FileEntity)entity.getKey())));
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
        return entity;
    }

    @Override
    public <E extends IEntity> boolean delete(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        Preconditions.checkArgument(connection != null && connection.state().isOpen());

        try {
            E entity = find(key, type, context);
            if (entity == null) {
                return false;
            }
            if (!((S3FileEntity)entity).remoteExists()) {
                throw new DataStoreException(String.format("Specified file doesn't exist. [key=%s]", entity.getKey().toString()));
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity> E find(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        return null;
    }

    @Override
    public <E extends IEntity> Collection<E> search(@Nonnull String query, int offset, int maxResults, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        return null;
    }

    @Override
    public <E extends IEntity> Collection<E> search(@Nonnull String query, int offset, int maxResults, Map<String, Object> parameters, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        return null;
    }

    @Override
    public void close() throws IOException {
        // Do nothing...
    }
}
