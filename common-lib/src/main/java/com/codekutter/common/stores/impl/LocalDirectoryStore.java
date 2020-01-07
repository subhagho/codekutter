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

import com.codekutter.common.model.FileEntity;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.StringEntity;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.AbstractDirectoryStore;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.DataStoreManager;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public class LocalDirectoryStore extends AbstractDirectoryStore<File> {
    @ConfigAttribute(name = "directory")
    private String directory;

    @Override
    public <S, T> void move(S source, T target, Object... params) throws DataStoreException {
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
    public void configure(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        Preconditions.checkArgument(config() != null);
        AbstractConnection<File> connection = dataStoreManager.getConnection(config().connectionName(), File.class);
        if (!(connection instanceof LocalDirectoryConnection)) {
            throw new ConfigurationException(String.format("No connection found for name. [name=%s]", config().connectionName()));
        }
        withConnection(connection);
    }

    @Override
    public <E extends IEntity> E create(@Nonnull E entity, @Nonnull Class<? extends IEntity> type, Object... params) throws DataStoreException {
        FileEntity e = null;
        if (entity instanceof FileEntity) {
            e = (FileEntity) entity;
        } else if (entity instanceof StringEntity) {
            String path = ((StringEntity) entity).getValue();
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
    public <E extends IEntity> E update(@Nonnull E entity, @Nonnull Class<? extends IEntity> type, Object... params) throws DataStoreException {
        return create(entity, type, params);
    }

    @Override
    public <E extends IEntity> boolean delete(@Nonnull Object key, @Nonnull Class<? extends E> type, Object... params) throws DataStoreException {
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
    public <E extends IEntity> E find(@Nonnull Object key, @Nonnull Class<? extends E> type, Object... params) throws DataStoreException {
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
    public <E extends IEntity> Collection<E> search(@Nonnull String query,
                                                    int offset,
                                                    int maxResults,
                                                    @Nonnull Class<? extends E> type,
                                                    Object... params) throws DataStoreException {
        if (!ReflectionUtils.isSuperType(FileEntity.class, type)) {
            throw new DataStoreException(String.format("Unsupported entity type. [type=%s]", type.getCanonicalName()));
        }
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
            return (Collection<E>) array;
        }
        return null;
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
    public <E extends IEntity> Collection<E> search(@Nonnull String query,
                                                    int offset,
                                                    int maxResults,
                                                    Map<String, Object> parameters,
                                                    @Nonnull Class<? extends E> type,
                                                    Object... params) throws DataStoreException {
        return search(query, offset, maxResults, type, params);
    }

    @Override
    public void close() throws IOException {

    }
}
