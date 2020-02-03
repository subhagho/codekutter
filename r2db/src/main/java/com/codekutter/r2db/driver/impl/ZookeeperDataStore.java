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
import com.codekutter.common.GlobalConstants;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.IKey;
import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.AbstractDataStore;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.DataStoreManager;
import com.codekutter.common.stores.annotations.Reference;
import com.codekutter.common.stores.impl.DataStoreAuditContext;
import com.codekutter.common.stores.impl.ZookeeperConnection;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.r2db.driver.impl.annotations.ZkEntity;
import com.codekutter.r2db.driver.impl.annotations.ZkProperty;
import com.codekutter.r2db.driver.model.ZkEntityReference;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Getter
@Setter
@Accessors(fluent = true)
public class ZookeeperDataStore extends AbstractDataStore<CuratorFramework> {

    @Override
    public void configureDataStore(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        Preconditions.checkState(config() instanceof ZookeeperDataStoreConfig);
        try {
            AbstractConnection<CuratorFramework> connection = dataStoreManager.getConnection(config().connectionName(), CuratorFramework.class);
            if (!(connection instanceof ZookeeperConnection)) {
                throw new ConfigurationException(String.format("Invalid/NULL connection specified. [name=%s][type=%s]",
                        config().connectionName(), CuratorFramework.class.getCanonicalName()));
            }
            withConnection(connection);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private <E extends IEntity> String getZkPath(Class<? extends E> type, IKey key) throws DataStoreException {
        ZookeeperDataStoreConfig config = (ZookeeperDataStoreConfig) config();
        if (!type.isAnnotationPresent(ZkEntity.class)) {
            throw new DataStoreException(String.format("Zookeeper Entity annotation not specified. [type=%s]", type.getCanonicalName()));
        }
        ZkEntity ze = type.getAnnotation(ZkEntity.class);
        String path = ze.path();
        boolean appendname = true;
        if (Strings.isNullOrEmpty(path)) {
            path = type.getCanonicalName().replaceAll("\\.", "/");
            appendname = false;
        }
        if (appendname) {
            String name = ze.name();
            if (Strings.isNullOrEmpty(name)) {
                name = type.getName();
            }
            path = String.format("%s/%s", path, name);
        }
        path = String.format("%s/%s", config.rootPath(), path);

        return String.format("%s/%s", path, key.stringKey());
    }

    @SuppressWarnings("rawtypes")
    private <E extends IEntity> E readEntityData(String path, Class<? extends E> type, Context context) throws DataStoreException {
        try {
            CuratorFramework client = connection().connection();
            Stat stat = client.checkExists().forPath(path);
            if (stat != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    Map properties = GlobalConstants.getJsonMapper().readValue(data, Map.class);
                    if (properties != null) {
                        E entity = type.newInstance();
                        Map<String, Field> fields = ReflectionUtils.getFieldsMap(type);
                        if (fields == null) {
                            throw new DataStoreException(String.format("Error getting fields for type. [type=%s]", type.getCanonicalName()));
                        }
                        for (Object key : properties.keySet()) {
                            String k = (String) key;
                            Object value = properties.get(key);
                            Field field = fields.get(k);
                            if (field == null) {
                                throw new DataStoreException(String.format("Field not found. [name=%s][type=%s]", k, type.getCanonicalName()));
                            }
                            if (ReflectionUtils.isPrimitiveTypeOrString(field)) {
                                ReflectionUtils.setObjectValue(entity, field, value);
                            } else {
                                Class<?> itype = field.getType();
                                if (ReflectionUtils.implementsInterface(List.class, itype)) {
                                    itype = ReflectionUtils.getGenericListType(field);
                                } else if (ReflectionUtils.implementsInterface(Set.class, itype)) {
                                    itype = ReflectionUtils.getGenericSetType(field);
                                }
                                boolean reference = false;
                                if (ReflectionUtils.implementsInterface(IEntity.class, itype)) {
                                    if (itype.isAnnotationPresent(ZkEntity.class)) {
                                        reference = true;
                                    }
                                }
                                if (!reference) {
                                    ReflectionUtils.setObjectValue(entity, field, value);
                                }
                            }
                        }
                    } else {
                        throw new DataStoreException(String.format("Error reading object data. [type=%s][path=%s]", type.getCanonicalName(), path));
                    }
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <E extends IEntity> E setEntityData(String path, E entity, Class<? extends E> type, Context context) throws DataStoreException {
        try {
            Map<String, Object> properties = new HashMap<>();
            Field[] fields = ReflectionUtils.getAllFields(type);
            if (fields != null && fields.length > 0) {
                for (Field field : fields) {
                    if (field.isAnnotationPresent(Reference.class))
                        continue;

                    boolean cascade = false;
                    if (field.isAnnotationPresent(ZkProperty.class)) {
                        ZkProperty zp = field.getAnnotation(ZkProperty.class);
                        if (zp.ignore()) continue;
                        cascade = zp.cascade();
                    }

                    Object value = ReflectionUtils.getFieldValue(entity, field);
                    if (ReflectionUtils.isPrimitiveTypeOrString(field)) {
                        String name = field.getName();
                        properties.put(name, value);
                    } else {
                        Class<?> itype = field.getType();
                        if (ReflectionUtils.implementsInterface(List.class, itype)) {
                            itype = ReflectionUtils.getGenericListType(field);
                        } else if (ReflectionUtils.implementsInterface(Set.class, itype)) {
                            itype = ReflectionUtils.getGenericSetType(field);
                        }
                        boolean reference = false;
                        if (ReflectionUtils.implementsInterface(IEntity.class, itype)) {
                            if (itype.isAnnotationPresent(ZkEntity.class)) {
                                if (ReflectionUtils.implementsInterface(List.class, field.getType())) {
                                    List<ZkEntityReference> references = new ArrayList<>();
                                    List values = (List) value;
                                    for (Object v : values) {
                                        IEntity ie = (IEntity) v;
                                        if (cascade) {
                                            String p = createZkEntity(ie, (Class<? extends IEntity>) itype, context);
                                            if (Strings.isNullOrEmpty(p)) {
                                                throw new DataStoreException(String.format("Error creating reference entity. [type=%s]", itype.getCanonicalName()));
                                            }
                                        }
                                        ZkEntityReference ref = new ZkEntityReference();
                                        ref.setType(itype.getCanonicalName());
                                        ref.setKey(ie.getKey());
                                        references.add(ref);
                                    }
                                    properties.put(field.getName(), references);
                                } else if (ReflectionUtils.implementsInterface(List.class, field.getType())) {
                                    Set<ZkEntityReference> references = new HashSet<>();
                                    Set values = (Set) value;
                                    for (Object v : values) {
                                        IEntity ie = (IEntity) v;
                                        if (cascade) {
                                            String p = createZkEntity(ie, (Class<? extends IEntity>) itype, context);
                                            if (Strings.isNullOrEmpty(p)) {
                                                throw new DataStoreException(String.format("Error creating reference entity. [type=%s]", itype.getCanonicalName()));
                                            }
                                        }
                                        ZkEntityReference ref = new ZkEntityReference();
                                        ref.setType(itype.getCanonicalName());
                                        ref.setKey(ie.getKey());
                                        references.add(ref);
                                    }
                                    properties.put(field.getName(), references);
                                }
                                reference = true;
                            }
                        }
                        if (!reference) {
                            String name = field.getName();
                            properties.put(name, value);
                        }
                    }
                }
            }
            CuratorFramework client = connection().connection();
            String json = GlobalConstants.getJsonMapper().writeValueAsString(properties);
            Stat stat = client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
            if (stat == null) {
                throw new DataStoreException(String.format("Error setting data for path. [path=%s]", path));
            }
            return entity;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private <E extends IEntity> String createZkEntity(E entity, Class<? extends E> type, Context context) throws DataStoreException {
        try {
            CuratorFramework client = connection().connection();
            entity.validate();
            String path = getZkPath(type, entity.getKey());
            Stat stat = client.checkExists().forPath(path);
            if (stat == null) {
                path = client.create().withMode(CreateMode.PERSISTENT).forPath(path);
                if (Strings.isNullOrEmpty(path)) {
                    throw new DataStoreException(String.format("Error creating zookeeper path. [path=%s]", path));
                }
            }
            setEntityData(path, entity, type, context);

            return path;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <E extends IEntity> E createEntity(@Nonnull E entity,
                                              @Nonnull Class<? extends E> type,
                                              Context context) throws DataStoreException {
        try {
            createZkEntity(entity, type, context);
            return entity;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    @Override
    public <E extends IEntity> E updateEntity(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        return createEntity(entity, type, context);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <E extends IEntity> boolean deleteZkEntity(String path, E entity, Class<? extends E> type, Context context) throws DataStoreException {
        try {
            CuratorFramework client = connection().connection();
            client.delete().forPath(path);
            Field[] fields = ReflectionUtils.getAllFields(type);
            if (fields != null && fields.length > 0) {
                for (Field field : fields) {
                    Class<?> itype = field.getType();
                    if (ReflectionUtils.implementsInterface(List.class, itype)) {
                        itype = ReflectionUtils.getGenericListType(field);
                    } else if (ReflectionUtils.implementsInterface(Set.class, itype)) {
                        itype = ReflectionUtils.getGenericSetType(field);
                    }
                    Object value = ReflectionUtils.getFieldValue(entity, field);
                    if (field.isAnnotationPresent(ZkProperty.class)) {
                        ZkProperty zp = field.getAnnotation(ZkProperty.class);
                        if (zp.cascade()) {
                            if (ReflectionUtils.implementsInterface(IEntity.class, itype)
                                    && itype.isAnnotationPresent(ZkEntity.class)) {
                                if (ReflectionUtils.implementsInterface(List.class, field.getType())) {
                                    List values = (List) value;
                                    for (Object v : values) {
                                        deleteEntity(((IEntity) v).getKey(), (Class<? extends IEntity>) itype, context);
                                    }
                                } else if (ReflectionUtils.implementsInterface(Set.class, field.getType())) {
                                    Set values = (Set) value;
                                    for (Object v : values) {
                                        deleteEntity(((IEntity) v).getKey(), (Class<? extends IEntity>) itype, context);
                                    }
                                } else {
                                    deleteEntity(((IEntity) value).getKey(), (Class<? extends IEntity>) itype, context);
                                }
                            }
                        }
                    }
                }
            }
            return true;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity> boolean deleteEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        Preconditions.checkArgument(key instanceof IKey);
        String path = getZkPath(type, (IKey) key);
        E entity = readEntityData(path, type, context);
        if (entity != null) {
            return deleteZkEntity(path, entity, type, context);
        }
        return false;
    }

    @Override
    public <E extends IEntity> E findEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        Preconditions.checkArgument(key instanceof IKey);
        String path = getZkPath(type, (IKey) key);
        return readEntityData(path, type, context);
    }

    @Override
    public <E extends IEntity> Collection<E> doSearch(@Nonnull String query, int offset, int maxResults, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        return null;
    }

    @Override
    public <E extends IEntity> Collection<E> doSearch(@Nonnull String query, int offset, int maxResults, Map<String, Object> parameters, @Nonnull Class<? extends E> type, Context context) throws DataStoreException {
        return null;
    }

    @Override
    public DataStoreAuditContext context() {
        return null;
    }
}
