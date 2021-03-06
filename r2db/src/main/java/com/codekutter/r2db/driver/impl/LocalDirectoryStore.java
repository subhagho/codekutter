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

import com.codekutter.common.Context;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.IKey;
import com.codekutter.common.model.StringEntity;
import com.codekutter.common.stores.*;
import com.codekutter.common.stores.impl.DataStoreAuditContext;
import com.codekutter.common.stores.impl.EntitySearchResult;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.r2db.driver.model.FileEntity;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.nio.file.attribute.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public class LocalDirectoryStore extends AbstractDirectoryStore<File> {
    private File directory;

    @Override
    public <S, T> void copy(S source, T target, Context context) throws DataStoreException {
        if (source instanceof File) {
            File td = null;
            if (target instanceof File) {
                td = (File) target;
            } else if (target instanceof String) {
                td = new File((String) target);
            }
            if (td != null) {
                if (!td.exists()) {
                    throw new DataStoreException(String.format("Target directory not found. [path=%s]", td.getAbsolutePath()));
                }
                String fname = FilenameUtils.getName(((File) source).getAbsolutePath());
                File tf = new File(Paths.get(td.getAbsolutePath(), fname).toString());
                File sf = (File) source;
                try {
                    FileUtils.copyFile(sf, tf);
                } catch (IOException ex) {
                    throw new DataStoreException(ex);
                }
            } else {
                throw new DataStoreException(String.format("Target type not supported. [type=%s]", target.getClass().getCanonicalName()));
            }
        } else {
            throw new DataStoreException(String.format("Source type not supported. [type=%s]", source.getClass().getCanonicalName()));
        }
    }

    @Override
    public <S, T> void move(S source, T target, Context context) throws DataStoreException {
        if (source instanceof File) {
            File td = null;
            if (target instanceof File) {
                td = (File) target;
            } else if (target instanceof String) {
                td = new File((String) target);
            }
            if (td != null) {
                if (!td.exists()) {
                    throw new DataStoreException(String.format("Target directory not found. [path=%s]", td.getAbsolutePath()));
                }
                String fname = FilenameUtils.getName(((File) source).getAbsolutePath());
                File tf = new File(Paths.get(td.getAbsolutePath(), fname).toString());
                if (!((File) source).renameTo(tf)) {
                    throw new DataStoreException(String.format("Error moving file to destination. [file=%s][destination=%s]", ((File) source).getAbsolutePath(), td.getAbsolutePath()));
                }
            } else {
                throw new DataStoreException(String.format("Target type not supported. [type=%s]", target.getClass().getCanonicalName()));
            }
        } else {
            throw new DataStoreException(String.format("Source type not supported. [type=%s]", source.getClass().getCanonicalName()));
        }
    }

    @Override
    public <K extends IKey, T extends IEntity<K>> T changeGroup(@Nonnull K key, @Nonnull String group, Context context) throws DataStoreException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        FileEntity entity = findEntity(key, FileEntity.class, context);
        if (entity == null) {
            throw new DataStoreException(String.format("File not found. [key=%s]", key.stringKey()));
        }
        try {
            GroupPrincipal grp = Files.readAttributes(entity.toPath(), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS).group();
            if (grp.getName().compareTo(group) != 0) {
                UserPrincipalLookupService lookupService = FileSystems.getDefault()
                        .getUserPrincipalLookupService();
                GroupPrincipal ngrp = lookupService.lookupPrincipalByGroupName(group);
                Files.getFileAttributeView(entity.toPath(), PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).setGroup(ngrp);
            }
            return (T) entity;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <K extends IKey, T extends IEntity<K>> T changeOwner(@Nonnull K key, @Nonnull String owner, Context context) throws DataStoreException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(owner));
        FileEntity entity = findEntity(key, FileEntity.class, context);
        if (entity == null) {
            throw new DataStoreException(String.format("File not found. [key=%s]", key.stringKey()));
        }
        try {
            UserPrincipal cu = Files.getOwner(entity.toPath());
            if (cu.getName().compareTo(owner) != 0) {
                UserPrincipalLookupService lookupService = FileSystems.getDefault()
                        .getUserPrincipalLookupService();
                UserPrincipal user = lookupService.lookupPrincipalByName(owner);
                Files.setOwner(entity.toPath(), user);
            }
            return (T) entity;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <K extends IKey, T extends IEntity<K>> T changePermission(@Nonnull K key, String permission, Context context) throws DataStoreException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(permission));
        FileEntity entity = findEntity(key, FileEntity.class, context);
        if (entity == null) {
            throw new DataStoreException(String.format("File not found. [key=%s]", key.stringKey()));
        }
        try {
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString(permission);
            Files.setPosixFilePermissions(entity.toPath(), perms);

            return (T) entity;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void configureDataStore(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        try {
            Preconditions.checkArgument(config() instanceof LocalDirStoreConfig);
            AbstractConnection<File> connection = dataStoreManager.getConnection(config().getConnectionName(), File.class);
            if (!(connection instanceof LocalDirectoryConnection)) {
                throw new ConfigurationException(String.format("No connection found for name. [name=%s]", config().getConnectionName()));
            }
            withConnection(connection);
            LocalDirStoreConfig config = (LocalDirStoreConfig) config();
            directory = new File(config.directory());
            if (!directory.exists() || !directory.isDirectory()) {
                throw new ConfigurationException(String.format("Specified directory not found. [path=%s]",
                        directory.getAbsolutePath()));
            }
            if (config.getMaxResults() > 0) {
                maxResults(config.getMaxResults());
            }
        } catch (DataStoreException ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public <E extends IEntity> E createEntity(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        FileEntity e = null;
        if (entity instanceof FileEntity) {
            e = (FileEntity) entity;
        } else if (entity instanceof StringEntity) {
            String path = ((StringEntity) entity).getValue().getKey();
            if (!Strings.isNullOrEmpty(path))
                e = new FileEntity(path);
        } else {
            throw new DataStoreException(String.format("Unsupported entity type. [type=%s]", entity.getClass().getCanonicalName()));
        }
        if (e == null) {
            throw new DataStoreException(String.format("Invalid entity specified. [type=%s][value=%s]", entity.getClass().getCanonicalName(), entity.toString()));
        }
        if (!e.exists()) {
            if (!e.mkdirs()) {
                throw new DataStoreException(String.format("Error creating entity. [path=%s]", e.getAbsolutePath()));
            }
        }
        return entity;
    }

    @Override
    public <E extends IEntity> E updateEntity(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        return createEntity(entity, type, context);
    }

    @Override
    public <E extends IEntity> boolean deleteEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        try {
            if (key instanceof String) {
                File file = new File((String) key);
                if (file.exists()) {
                    if (file.isDirectory()) {
                        FileUtils.deleteDirectory(file);
                    } else {
                        return file.delete();
                    }
                }
            } else {
                throw new DataStoreException(String.format("Unsupported key type. [type=%s]", key.getClass().getCanonicalName()));
            }
            return false;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity> E findEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        if (key instanceof String) {
            File file = new File((String) key);
            if (file.exists()) {
                return (E) new FileEntity(file.getAbsolutePath());
            }
        } else {
            throw new DataStoreException(String.format("Unsupported key type. [type=%s]", key.getClass().getCanonicalName()));
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query,
                                                            int offset,
                                                            int maxResults,
                                                            @Nonnull Class<? extends E> type,
                                                            Context context) throws DataStoreException {
        if (!ReflectionUtils.isSuperType(FileEntity.class, type)) {
            throw new DataStoreException(String.format("Unsupported entity type. [type=%s]", type.getCanonicalName()));
        }
        try {
            FileFilter filter = null;
            if (query.trim().compareTo("*") != 0) {
                filter = new RegexFileFilter(query);
            }
            List<FileEntity> array = new ArrayList<>();
            array = findRecursive(connection().connection(), filter, array);
            if (offset > 0) {
                if (offset < array.size()) {
                    array = array.subList(offset, (array.size() - 1));
                }
            }
            if (maxResults <= 0) {
                maxResults = maxResults();
            }
            if (array.size() > maxResults) {
                array = array.subList(0, maxResults - 1);
            }
            if (!array.isEmpty()) {
                EntitySearchResult<FileEntity> er = new EntitySearchResult<>(type);
                er.setQuery(query);
                er.setOffset(offset);
                er.setCount(array.size());
                er.setEntities(array);
                return (BaseSearchResult<E>) er;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private List<FileEntity> findRecursive(File dir, FileFilter filter, List<FileEntity> array) {
        File[] files = null;
        if (filter == null) {
            files = dir.listFiles();
        } else {
            files = dir.listFiles(filter);
        }
        if (files != null && files.length > 0) {
            for (File file : files) {
                array.add(new FileEntity(file.getAbsolutePath()));
                if (file.isDirectory()) {
                    findRecursive(file, filter, array);
                }
            }
        }
        return array;
    }

    @Override
    public <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query,
                                                            int offset,
                                                            int maxResults,
                                                            Map<String, Object> parameters,
                                                            @Nonnull Class<? extends E> type,
                                                            Context context) throws DataStoreException {
        return doSearch(query, offset, maxResults, type, context);
    }

    @Override
    public DataStoreAuditContext context() {
        FileDataStoreContext ctx = new FileDataStoreContext();
        ctx.setType(getClass().getCanonicalName());
        ctx.setName(name());
        ctx.setConnectionType(connection().type().getCanonicalName());
        ctx.setConnectionName(connection().name());
        ctx.setDirectory(directory.getAbsolutePath());
        return ctx;
    }
}
