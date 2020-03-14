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

package com.codekutter.r2db.tools;

import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.ConnectionException;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.r2db.driver.impl.ElasticSearchConnection;
import com.codekutter.r2db.driver.impl.annotations.Indexed;
import com.codekutter.r2db.driver.model.Searchable;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.*;

@Getter
@Setter
@Accessors(fluent = true)
public class IndexBuilder {
    public static final String MAPPING_PROPERTIES = "properties";
    public static final String MAPPING_TYPE = "type";
    public static final String MAPPING_ANALYZER = "analyzer";
    public static final String MAPPING_FIELD_DATA = "fielddata";
    public static final String SETTING_WRITE_SHARDS = "index.write.wait_for_active_shards";
    public static final String SETTING_NUM_SHARDS = "index.number_of_shards";
    public static final String SETTING_NUM_REPLICAS = "index.number_of_replicas";

    private int minWriteShards = 1;
    private int maxShards = 1;
    private int replicas = 1;
    private ElasticSearchConnection connection;
    private String index;

    public IndexBuilder withConnection(String searchConnection) throws DataStoreException {
        connection = (ElasticSearchConnection) ConnectionManager.get().connection(searchConnection, RestHighLevelClient.class);
        if (connection == null) {
            throw new DataStoreException(String.format("Connection not found. [name=%s][type=%s]", searchConnection, ElasticSearchConnection.class.getCanonicalName()));
        }
        return this;
    }

    public void setupIndex(@Nonnull Class<? extends IEntity> type) throws DataStoreException {
        Preconditions.checkState(connection != null);
        try {
            if (!type.isAnnotationPresent(Indexed.class)) {
                throw new DataStoreException(String.format("Type is not indexed. [type=%s]", type.getCanonicalName()));
            }
            Indexed indexed = type.getAnnotation(Indexed.class);
            index = indexed.index();
            if (Strings.isNullOrEmpty(index)) {
                index = type.getCanonicalName();
            }

            if (!indexExists()) {
                LogUtils.info(getClass(), String.format("Creating new index. [index=%s]", index));
                createIndex(type, indexed);
            } else {
                LogUtils.info(getClass(), String.format("Updating index. [index=%s]", index));
                updateIndex(type, indexed);
            }

        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            throw new DataStoreException(ex);
        }
    }

    public void closeIndex() throws Exception {
        RestHighLevelClient client = connection.connection();
        CloseIndexRequest request = new CloseIndexRequest(index);
        AcknowledgedResponse response = client.indices().close(request, RequestOptions.DEFAULT);
        if (!response.isAcknowledged()) {
            throw new Exception(String.format("Index close not acked. [index=%s]", index));
        }
    }

    public void openIndex() throws Exception {
        RestHighLevelClient client = connection.connection();
        OpenIndexRequest request = new OpenIndexRequest(index);
        AcknowledgedResponse response = client.indices().open(request, RequestOptions.DEFAULT);
        if (!response.isAcknowledged()) {
            throw new Exception(String.format("Index open not acked. [index=%s]", index));
        }
    }

    public void deleteIndex() throws Exception {
        RestHighLevelClient client = connection.connection();
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        if (!response.isAcknowledged()) {
            throw new Exception(String.format("Index open not acked. [index=%s]", index));
        }
    }

    private void updateIndex(Class<? extends IEntity> type, Indexed indexed) throws Exception {
        deleteIndex();
        createIndex(type, indexed);
    }

    private void createIndex(Class<? extends IEntity> type, Indexed indexed) throws Exception {
        RestHighLevelClient client = connection.connection();
        Field[] fields = ReflectionUtils.getAllFields(type);
        if (fields != null && fields.length > 0) {
            CreateIndexRequest indexRequest = new CreateIndexRequest(index);
            String analyzer = String.format("analyzer_%s_%s", index, indexed.language());
            settings(analyzer, index, indexed, indexRequest);
            Map<String, Object> properties = new HashMap<>();
            for (Field field : fields) {
                processField(null, analyzer, field, properties);
            }
            if (!properties.isEmpty()) {
                Map<String, Object> mappings = new HashMap<>();
                mappings.put(MAPPING_PROPERTIES, properties);
                indexRequest.mapping(mappings);
            }
            CreateIndexResponse response = client.indices().create(indexRequest, RequestOptions.DEFAULT);
            if (!response.isShardsAcknowledged()) {
                throw new Exception(String.format("Shard response not received. [index=%s]", index));
            }
            LogUtils.info(getClass(), String.format("Index created. [name=%s]", index));
        }
    }

    public boolean indexExists() throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(index));

        RestHighLevelClient client = connection.connection();

        GetIndexRequest request = new GetIndexRequest(index);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    private void settings(String analyzer, String index, Indexed indexed, CreateIndexRequest request) throws Exception {
        String analysisJson = IndexCreateHelper.analysisSetting(analyzer, indexed);
        if (Strings.isNullOrEmpty(analysisJson)) {
            throw new Exception(String.format("Error getting Analysis JSON for index. [name=%s]", index));
        }
        request.settings(Settings.builder()
                .put(SETTING_NUM_REPLICAS, replicas)
                .put(SETTING_NUM_SHARDS, maxShards)
                .put(SETTING_WRITE_SHARDS, minWriteShards)
                .loadFromSource(analysisJson, XContentType.JSON));
    }

    private void processField(String parent, String analyzer, Field field, Map<String, Object> properties) throws Exception {
        if (field.isAnnotationPresent(JsonIgnore.class)) return;
        String fname = field.getName();
        if (!Strings.isNullOrEmpty(parent)) {
            fname = parent + "." + field.getName();
        }

        if (field.isAnnotationPresent(Searchable.class)) {
            Searchable searchable = field.getAnnotation(Searchable.class);
            if (searchable.faceted()) {
                if (ReflectionUtils.isPrimitiveTypeOrString(field)
                        || field.getType().equals(Date.class)
                        || field.getType().equals(Timestamp.class)) {
                    String dt = IndexCreateHelper.parseFieldType(field);
                    if (Strings.isNullOrEmpty(dt)) {
                        throw new Exception(String.format("Error getting datatype for field. [type=%s]", field.getType().getCanonicalName()));
                    }
                    Map<String, Object> fmap = new HashMap<>();
                    fmap.put(MAPPING_TYPE, dt);
                    fmap.put(MAPPING_ANALYZER, analyzer);
                    fmap.put(MAPPING_FIELD_DATA, true);
                    properties.put(fname, fmap);
                } else {
                    throw new Exception(
                            String.format("Field type cannot be faceted. [type=%s]", field.getType().getCanonicalName()));
                }
            }
        } else if (!ReflectionUtils.isPrimitiveTypeOrString(field)
                && !field.getType().equals(Date.class)
                && !field.getType().equals(Timestamp.class)) {
            Class<?> ft = field.getType();
            if (ReflectionUtils.implementsInterface(List.class, ft)) {
                ft = ReflectionUtils.getGenericListType(field);
            } else if (ReflectionUtils.implementsInterface(Set.class, ft)) {
                ft = ReflectionUtils.getGenericSetType(field);
            }
            Field[] fields = ReflectionUtils.getAllFields(ft);
            if (fields != null && fields.length > 0) {
                for (Field f : fields) {
                    processField(fname, analyzer, f, properties);
                }
            }
        }
    }

    public void printSettings() throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(index));
        RestHighLevelClient client = connection.connection();

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
        GetSettingsResponse indexResponse = client.indices().getSettings(getSettingsRequest, RequestOptions.DEFAULT);
        Settings settings = indexResponse.getIndexToSettings().get(index);

        for (String key : settings.keySet()) {
            System.out.println(key + " : " + settings.get(key));
        }
    }

    public void printMappings() throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(index));
        RestHighLevelClient client = connection.connection();

        GetMappingsRequest request = new GetMappingsRequest();
        GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
        Map<String, MappingMetaData> allMappings = response.mappings();
        MappingMetaData indexMapping = allMappings.get(index);
        Map<String, Object> mapping = indexMapping.sourceAsMap();
        for(String key : mapping.keySet()) {
            System.out.println(String.format("%s ==> [%s]", key, mapping.get(key)));
        }
    }
}
