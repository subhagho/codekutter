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
import com.amazonaws.services.s3.model.*;
import com.codekutter.common.Context;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.AbstractDirectoryStore;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.DataStoreManager;
import com.codekutter.common.stores.impl.DataStoreAuditContext;
import com.codekutter.common.utils.IOUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.r2db.driver.model.S3FileEntity;
import com.codekutter.r2db.driver.model.S3FileKey;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.*;

@Getter
@Accessors(fluent = true)
public class AwsS3DataStore extends AbstractDirectoryStore<AmazonS3> {
    private File workDirectory;
    private String bucket;

    @Override
    public <S, T> void move(S source, T target, Context context) throws DataStoreException {
        Preconditions.checkArgument(source instanceof S3FileKey);
        Preconditions.checkArgument(target instanceof S3FileKey);
        Preconditions.checkArgument(connection() != null && connection().state().isOpen());

        S3FileKey sk = (S3FileKey) source;
        S3FileKey tk = (S3FileKey) target;
        try {
            CopyObjectRequest copyObjRequest = new CopyObjectRequest(sk.bucket(), sk.key(), tk.bucket(), tk.key());
            connection().connection().copyObject(copyObjRequest);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void configureDataStore(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        Preconditions.checkState(config() instanceof S3StoreConfig);
        try {
            AbstractConnection<AmazonS3> connection =
                    dataStoreManager.getConnection(config().connectionName(), AmazonS3.class);
            if (!(connection instanceof AwsS3Connection)) {
                throw new ConfigurationException(String.format("No connection found for name. [name=%s]", config().connectionName()));
            }
            withConnection(connection);

            S3StoreConfig config = (S3StoreConfig) config();
            this.bucket = config.bucket();
            String wd = config.tempDirectory();
            if (Strings.isNullOrEmpty(wd)) {
                wd = IOUtils.getTempDirectory(UUID.randomUUID().toString());
            } else {
                wd = String.format("%s/%s", wd, UUID.randomUUID().toString());
            }
            workDirectory = new File(wd);
            if (!workDirectory.exists()) {
                if (!workDirectory.mkdirs()) {
                    throw new ConfigurationException(String.format("Error creating working directory. [path=%s]", workDirectory.getAbsolutePath()));
                }
            }

        } catch (Throwable t) {
            throw new ConfigurationException(t);
        }
    }

    @Override
    public <E extends IEntity> E createEntity(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        Preconditions.checkArgument(entity instanceof S3FileEntity);
        Preconditions.checkArgument(connection() != null && connection().state().isOpen());

        try {
            String key = ((S3FileEntity) entity).withClient(connection().connection()).copyToRemote();
            if (Strings.isNullOrEmpty(key)) {
                throw new DataStoreException(String.format("Error uploading file to S3. [key=%s]", entity.getKey().stringKey()));
            }
            ((S3FileEntity) entity).setUpdateTimestamp(System.currentTimeMillis());
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
        return entity;
    }

    @Override
    public <E extends IEntity> E updateEntity(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        Preconditions.checkArgument(entity instanceof S3FileEntity);
        Preconditions.checkArgument(connection() != null && connection().state().isOpen());

        try {
            if (!((S3FileEntity) entity).remoteExists()) {
                throw new DataStoreException(String.format("Specified file doesn't exist. [key=%s]", entity.getKey().stringKey()));
            }
            String key = ((S3FileEntity) entity).withClient(connection().connection()).copyToRemote();
            if (Strings.isNullOrEmpty(key)) {
                throw new DataStoreException(String.format("Error uploading file to S3. [key=%s]", entity.getKey().stringKey()));
            }
            ((S3FileEntity) entity).setUpdateTimestamp(System.currentTimeMillis());
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
        return entity;
    }

    @Override
    public <E extends IEntity> boolean deleteEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        Preconditions.checkArgument(connection() != null && connection().state().isOpen());

        try {
            E entity = findEntity(key, type, context);
            if (entity == null) {
                return false;
            }
            boolean ret = false;
            S3FileEntity fe = (S3FileEntity) entity;
            if (fe.exists()) {
                ret = fe.delete();
                if (!ret) {
                    LogUtils.warn(getClass(), String.format("Error deleting local copy. [path=%s]", fe.getAbsolutePath()));
                }
            }
            if (fe.remoteExists()) {
                ret = fe.remoteDelete();
                if (!ret) {
                    throw new DataStoreException(String.format("Error deleting remote copy. [path=%s]", fe.getRemotePath()));
                }
            }

            return ret;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity> E findEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        Preconditions.checkArgument(key instanceof S3FileKey);
        Preconditions.checkArgument(ReflectionUtils.isSuperType(S3FileEntity.class, type));
        try {
            S3FileKey fk = (S3FileKey) key;
            S3Object obj = connection().connection().getObject(fk.bucket(), fk.key());
            if (obj != null) {
                String lfname = getLocalFileName(fk);
                S3FileEntity entity = new S3FileEntity(fk.bucket(), fk.key(), lfname).
                        withClient(connection().connection());
                ObjectMetadata meta = obj.getObjectMetadata();
                entity.setUpdateTimestamp(meta.getLastModified().getTime());

                File lf = entity.copyToLocal();
                if (!lf.exists()) {
                    throw new DataStoreException(String.format("Local file not found. [path=%s]", lf.getAbsolutePath()));
                }
                return (E) entity;
            }
            return null;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity> Collection<E> doSearch(@Nonnull String query,
                                                      int offset,
                                                      int maxResults,
                                                      @Nonnull Class<? extends E> type,
                                                      Context context) throws DataStoreException {
        Preconditions.checkArgument(ReflectionUtils.isSuperType(S3FileEntity.class, type));
        return doSearch(bucket, query, offset, maxResults, type, context);
    }

    protected  <E extends IEntity> Collection<E> doSearch(@Nonnull String bucket,
                                                      @Nonnull String query,
                                                      int offset,
                                                      int maxResults,
                                                      @Nonnull Class<? extends E> type,
                                                      Context context) throws DataStoreException {
        Preconditions.checkArgument(ReflectionUtils.isSuperType(S3FileEntity.class, type));
        try {
            S3StoreContext ctx = (S3StoreContext) context;

            AmazonS3 client = connection().connection();
            ListObjectsV2Request request = new ListObjectsV2Request();
            request.setBucketName(bucket);
            if (!Strings.isNullOrEmpty(query)) {
                request.setPrefix(query);
            }
            request.setMaxKeys(maxResults > 0 ? maxResults + offset : maxResults() + offset);
            if (ctx != null) {
                String ckey = ctx.continuationKey();
                if (!Strings.isNullOrEmpty(ckey)) {
                    request.setContinuationToken(ckey);
                }
            }
            ListObjectsV2Result result = client.listObjectsV2(request);
            if (result != null) {
                List<S3ObjectSummary> objs = result.getObjectSummaries();
                if (objs != null && !objs.isEmpty()) {
                    List<E> array = new ArrayList<>();
                    int count = 0;
                    for (S3ObjectSummary obj : objs) {
                        if (offset > 0 && count < offset) {
                            count++;
                            continue;
                        }

                        S3FileKey key = new S3FileKey();
                        key.bucket(obj.getBucketName());
                        key.key(obj.getKey());

                        S3FileEntity entity = findEntity(key, S3FileEntity.class, null);
                        if (entity == null) {
                            throw new DataStoreException(String.format("invalid key : [key=%s]", key.key()));
                        }
                        array.add((E) entity);
                        count++;
                        if ((count - offset) > maxResults) break;
                    }
                    return array;
                }
            }
            return null;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    @Override
    public <E extends IEntity> Collection<E> doSearch(@Nonnull String query, int offset, int maxResults,
                                                      Map<String, Object> parameters,
                                                      @Nonnull Class<? extends E> type,
                                                      Context context) throws DataStoreException {
        return doSearch(query, offset, maxResults, type, context);
    }

    @Override
    public DataStoreAuditContext context() {
        S3DataStoreAuditContext ctx = new S3DataStoreAuditContext();
        ctx.setType(getClass().getCanonicalName());
        ctx.setName(name());
        ctx.setConnectionType(connection().type().getCanonicalName());
        ctx.setConnectionName(connection().name());
        ctx.setBucket(bucket);
        return ctx;
    }

    private String getLocalFileName(S3FileKey key) {
        return String.format("%s/bucket-%s/%s", workDirectory.getAbsolutePath(), key.bucket(), key.key());
    }
}
