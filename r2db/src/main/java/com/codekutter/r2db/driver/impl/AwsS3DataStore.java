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
import com.codekutter.common.model.IKey;
import com.codekutter.common.stores.*;
import com.codekutter.common.stores.impl.DataStoreAuditContext;
import com.codekutter.common.stores.impl.EntitySearchResult;
import com.codekutter.common.utils.CypherUtils;
import com.codekutter.common.utils.IOUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.r2db.driver.model.S3FileEntity;
import com.codekutter.r2db.driver.model.S3FileKey;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public class AwsS3DataStore extends AbstractDirectoryStore<AmazonS3> {
    private File workDirectory;
    private String bucket;
    private Cache<S3FileKey, S3FileEntity> cache;

    @Override
    public <S, T> void move(S source, T target, Context context) throws DataStoreException {
        Preconditions.checkArgument(source instanceof S3FileKey);
        Preconditions.checkArgument(target instanceof S3FileKey);
        Preconditions.checkArgument(connection() != null && connection().state().isOpen());

        S3FileKey sk = (S3FileKey) source;
        S3FileKey tk = (S3FileKey) target;
        try {
            S3FileEntity entity = findEntity(sk, S3FileEntity.class, context);
            if (entity == null) {
                throw new DataStoreException(String.format("Entity not found. [key=%s]", sk.stringKey()));
            }
            CopyObjectRequest copyObjRequest = new CopyObjectRequest(sk.bucket(), sk.key(), tk.bucket(), tk.key());
            connection().connection().copyObject(copyObjRequest);
            entity = findEntity(tk, S3FileEntity.class, context);
            if (entity == null) {
                throw new DataStoreException(String.format("Error copying remote file. [key=%s]", tk.stringKey()));
            }
            deleteEntity(sk, S3FileEntity.class, context);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <K extends IKey, T extends IEntity<K>> T changeGroup(@Nonnull K key, @Nonnull String group, Context context) throws DataStoreException {
        S3FileEntity entity = find(key, S3FileEntity.class, context);
        if (entity != null) {
            try {
                // Get the existing object ACL that we want to modify.
                AccessControlList acl = connection().connection().getObjectAcl(entity.getKey().bucket(), entity.getKey().key());
                List<Grant> grants = acl.getGrantsAsList();
                List<Grant> remove = new ArrayList<>();
                if (grants != null && !grants.isEmpty()) {
                    for (Grant grant : grants) {
                        if (grant.getPermission() == Permission.FullControl) {
                            if (!(grant.getGrantee() instanceof CanonicalGrantee)) {
                                remove.add(grant);
                            }
                        }
                    }
                }
                if (!remove.isEmpty()) {
                    for(Grant grant : remove) {
                        acl.getGrantsAsList().remove(grant);
                    }
                }
                acl.grantPermission(new EmailAddressGrantee(group), Permission.FullControl);
                connection().connection().setObjectAcl(entity.getKey().bucket(), entity.getKey().key(), acl);

                return (T) entity;
            } catch (Exception ex) {
                throw new DataStoreException(ex);
            }
        }
        return null;
    }

    @Override
    public <K extends IKey, T extends IEntity<K>> T changeOwner(@Nonnull K key, @Nonnull String owner, Context context) throws DataStoreException {
        return changeGroup(key, owner, context);
    }

    @Override
    public <K extends IKey, T extends IEntity<K>> T changePermission(@Nonnull K key, String permission, Context context) throws DataStoreException {
        return null;
    }

    @Override
    public <S, T> void copy(S source, T target, Context context) throws DataStoreException {
        Preconditions.checkArgument(source instanceof S3FileKey);
        Preconditions.checkArgument(target instanceof S3FileKey);
        Preconditions.checkArgument(connection() != null && connection().state().isOpen());

        S3FileKey sk = (S3FileKey) source;
        S3FileKey tk = (S3FileKey) target;
        try {
            S3FileEntity entity = findEntity(sk, S3FileEntity.class, context);
            if (entity == null) {
                throw new DataStoreException(String.format("Entity not found. [key=%s]", sk.stringKey()));
            }
            CopyObjectRequest copyObjRequest = new CopyObjectRequest(sk.bucket(), sk.key(), tk.bucket(), tk.key());
            connection().connection().copyObject(copyObjRequest);
            entity = findEntity(tk, S3FileEntity.class, context);
            if (entity == null) {
                throw new DataStoreException(String.format("Error copying remote file. [key=%s]", tk.stringKey()));
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void configureDataStore(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        Preconditions.checkState(config() instanceof S3StoreConfig);
        try {
            AbstractConnection<AmazonS3> connection =
                    dataStoreManager.getConnection(config().getConnectionName(), AmazonS3.class);
            if (!(connection instanceof AwsS3Connection)) {
                throw new ConfigurationException(String.format("No connection found for name. [name=%s]", config().getConnectionName()));
            }
            withConnection(connection);

            S3StoreConfig config = (S3StoreConfig) config();
            this.bucket = config.getBucket();
            String wd = config.getTempDirectory();
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

            if (config.isUseCache()) {
                RemovalListener<S3FileKey, S3FileEntity> listener = new RemovalListener<S3FileKey, S3FileEntity>() {
                    @Override
                    public void onRemoval(RemovalNotification<S3FileKey, S3FileEntity> notification) {
                        S3FileEntity entity = notification.getValue();
                        if (entity.exists()) {
                            if (!entity.delete()) {
                                LogUtils.warn(getClass(), String.format("Failed to remove local file. [path=%s]", entity.getAbsolutePath()));
                            }
                        }
                    }
                };
                cache = CacheBuilder.newBuilder()
                        .maximumSize(config.getMaxCacheSize())
                        .expireAfterAccess(config.getCacheExpiryWindow(), TimeUnit.MILLISECONDS)
                        .removalListener(listener)
                        .build();
            }
            if (config.getMaxResults() > 0) {
                maxResults(config.getMaxResults());
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
            S3FileEntity fe = (S3FileEntity) entity;
            if (fe.withClient(connection().connection()).remoteExists()) {
                throw new DataStoreException(String.format("Duplicate file: Remote file with key already exists. [key=%s]", fe.getKey().key()));
            }
            String key = fe.copyToRemote();
            if (Strings.isNullOrEmpty(key)) {
                throw new DataStoreException(String.format("Error uploading file to S3. [key=%s]", entity.getKey().stringKey()));
            }
            fe.setUpdateTimestamp(System.currentTimeMillis());
            if (cache != null) {
                cache.put(fe.getKey(), fe);
            }
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
            S3FileEntity fe = (S3FileEntity) entity;

            if (!fe.withClient(connection().connection()).remoteExists()) {
                throw new DataStoreException(String.format("Specified file doesn't exist. [key=%s]", entity.getKey().stringKey()));
            }
            fe.setUpdateTimestamp(System.currentTimeMillis());
            String key = fe.copyToRemote();
            if (Strings.isNullOrEmpty(key)) {
                throw new DataStoreException(String.format("Error uploading file to S3. [key=%s]", entity.getKey().stringKey()));
            }
            if (cache != null) {
                cache.put(fe.getKey(), fe);
            }
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
            if (cache != null) {
                cache.invalidate(fe.getKey());
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
            if (cache != null) {
                S3FileEntity entity = cache.getIfPresent(key);
                if (entity != null) {
                    return (E) entity;
                }
            }
            S3Object obj = connection().connection().getObject(fk.bucket(), fk.key());
            if (obj != null) {
                try {
                    String lfname = getLocalFileName(fk);
                    S3FileEntity entity = new S3FileEntity(fk.bucket(), fk.key(), lfname).
                            withClient(connection().connection());
                    ObjectMetadata meta = obj.getObjectMetadata();
                    entity.setUpdateTimestamp(meta.getLastModified().getTime());

                    File lf = entity.copyToLocal();
                    if (!lf.exists()) {
                        throw new DataStoreException(String.format("Local file not found. [path=%s]", lf.getAbsolutePath()));
                    }
                    if (cache != null) {
                        cache.put(entity.getKey(), entity);
                    }
                    return (E) entity;
                } finally {
                    obj.close();
                }
            }
            return null;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query,
                                                            int offset,
                                                            int maxResults,
                                                            @Nonnull Class<? extends E> type,
                                                            Context context) throws DataStoreException {
        Preconditions.checkArgument(ReflectionUtils.isSuperType(S3FileEntity.class, type));
        return doSearch(bucket, query, offset, maxResults, type, context);
    }

    protected <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String bucket,
                                                               @Nonnull String query,
                                                               int offset,
                                                               int maxResults,
                                                               @Nonnull Class<? extends E> type,
                                                               Context context) throws DataStoreException {
        Preconditions.checkArgument(ReflectionUtils.isSuperType(S3FileEntity.class, type));
        try {
            if (maxResults <= 0) {
                maxResults = this.maxResults();
            }
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
                    List<S3FileEntity> array = new ArrayList<>();
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
                        array.add(entity);
                        count++;
                        if ((count - offset) > maxResults) break;
                    }
                    EntitySearchResult<S3FileEntity> er = new EntitySearchResult<>(type);
                    er.setOffset(offset);
                    er.setQuery(query);
                    er.setCount(array.size());
                    er.setEntities(array);

                    return (BaseSearchResult<E>) er;
                }
            }
            return null;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    @Override
    public <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query, int offset, int maxResults,
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

    private String getLocalFileName(S3FileKey key) throws Exception {
        String fname = FilenameUtils.getName(key.key());
        if (Strings.isNullOrEmpty(fname)) {
            // Should not happen. Will only happen due to null keys created in S3.
            fname = "ERROR.null";
        }
        String dname = FilenameUtils.getPath(key.key());
        String hash = CypherUtils.getKeyHash(dname);
        hash = hash.replaceAll("==", "");
        return String.format("%s/bucket_%s/%s/%s", workDirectory.getAbsolutePath(), key.bucket(), hash, fname);
    }

    @Override
    public void close() throws IOException {
        FileUtils.deleteDirectory(workDirectory);
        super.close();
    }
}
