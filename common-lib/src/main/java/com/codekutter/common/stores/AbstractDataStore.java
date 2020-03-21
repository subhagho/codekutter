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

package com.codekutter.common.stores;

import com.codekutter.common.Context;
import com.codekutter.common.auditing.AbstractAuditLogger;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.impl.DataStoreAuditContext;
import com.codekutter.common.utils.KeyValuePair;
import com.codekutter.common.utils.Monitoring;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Timer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AbstractDataStore<T> implements Closeable {

    private static final int DEFAULT_MAX_RESULTS = 500;
    @Setter(AccessLevel.NONE)
    protected Metrics metrics = new Metrics();
    @ConfigAttribute(name = "name", required = true)
    private String name;
    @ConfigValue(name = "maxResults")
    private int maxResults = DEFAULT_MAX_RESULTS;
    @Setter(AccessLevel.NONE)
    private AbstractConnection<T> connection = null;
    @Setter(AccessLevel.NONE)
    private long threadId;
    @Setter(AccessLevel.NONE)
    private DataStoreConfig config;
    @SuppressWarnings("rawtypes")
    private AbstractAuditLogger auditLogger;
    @Setter(AccessLevel.NONE)
    private DataStoreManager dataStoreManager;

    public AbstractDataStore() {
        threadId = Thread.currentThread().getId();
    }

    public void setupMonitoring() {
        metrics.createLatency = Monitoring.addTimer(String.format(metrics.METRIC_LATENCY_CREATE, getClass().getCanonicalName(), name()));
        metrics.updateLatency = Monitoring.addTimer(String.format(metrics.METRIC_LATENCY_UPDATE, getClass().getCanonicalName(), name()));
        metrics.deleteLatency = Monitoring.addTimer(String.format(metrics.METRIC_LATENCY_DELETE, getClass().getCanonicalName(), name()));
        metrics.readLatency = Monitoring.addTimer(String.format(metrics.METRIC_LATENCY_READ, getClass().getCanonicalName(), name()));
        metrics.searchLatency = Monitoring.addTimer(String.format(metrics.METRIC_LATENCY_SEARCH, getClass().getCanonicalName(), name()));

        metrics.createCounter = Monitoring.addCounter(String.format(metrics.METRIC_COUNTER_CREATE, getClass().getCanonicalName(), name()));
        metrics.updateCounter = Monitoring.addCounter(String.format(metrics.METRIC_COUNTER_UPDATE, getClass().getCanonicalName(), name()));
        metrics.deleteCounter = Monitoring.addCounter(String.format(metrics.METRIC_COUNTER_DELETE, getClass().getCanonicalName(), name()));
        metrics.readCounter = Monitoring.addCounter(String.format(metrics.METRIC_COUNTER_READ, getClass().getCanonicalName(), name()));
        metrics.searchCounter = Monitoring.addCounter(String.format(metrics.METRIC_COUNTER_SEARCH, getClass().getCanonicalName(), name()));

        metrics.createCounterErrors = Monitoring.addCounter(String.format(metrics.METRIC_COUNTER_ERROR_CREATE, getClass().getCanonicalName(), name()));
        metrics.updateCounterErrors = Monitoring.addCounter(String.format(metrics.METRIC_COUNTER_ERROR_UPDATE, getClass().getCanonicalName(), name()));
        metrics.deleteCounterErrors = Monitoring.addCounter(String.format(metrics.METRIC_COUNTER_ERROR_DELETE, getClass().getCanonicalName(), name()));
        metrics.readCounterErrors = Monitoring.addCounter(String.format(metrics.METRIC_COUNTER_READ_ERROR, getClass().getCanonicalName(), name()));
        metrics.searchCounterErrors = Monitoring.addCounter(String.format(metrics.METRIC_COUNTER_SEARCH_ERROR, getClass().getCanonicalName(), name()));

        metrics.TAGS[0] = new KeyValuePair<>();
        metrics.TAGS[1] = new KeyValuePair<>();
    }

    protected void checkThread() throws DataStoreException {
        long threadId = Thread.currentThread().getId();
        if (this.threadId != threadId) {
            throw new DataStoreException(String.format("Thread instance exception. [expected=%d][current=%d]", this.threadId, threadId));
        }
    }

    public AbstractDataStore<T> withConfig(@Nonnull DataStoreConfig config) {
        this.config = config;
        return this;
    }

    public AbstractDataStore<T> withConnection(@Nonnull AbstractConnection<T> connection) {
        this.connection = connection;
        return this;
    }

    public void configure(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        try {
            this.dataStoreManager = dataStoreManager;
            configureDataStore(dataStoreManager);
            setupMonitoring();
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public abstract void configureDataStore(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException;

    private void setTag(String value, Class<? extends IEntity> type) {
        metrics.TAGS[0].key(Metrics.TAG_OPERATION);
        metrics.TAGS[0].value(value);

        if (type != null) {
            metrics.TAGS[1].key(Metrics.TAG_ENTITY);
            metrics.TAGS[1].value(type.getCanonicalName());
        }
    }

    @SuppressWarnings("rawtypes")
    public <E extends IEntity> E create(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException {
        setTag(Metrics.METRIC_TAG_CREATE, type);
        try {
            Monitoring.increment(metrics.createCounter.name(), metrics.TAGS);
            return metrics.createLatency.record(() -> createEntity(entity, type, context));
        } catch (Throwable t) {
            Monitoring.increment(metrics.createCounterErrors.name(), metrics.TAGS);
            throw new DataStoreException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> E createEntity(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public <E extends IEntity> E update(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException {
        setTag(Metrics.METRIC_TAG_UPDATE, type);
        try {
            Monitoring.increment(metrics.updateCounter.name(), metrics.TAGS);
            return metrics.updateLatency.record(() -> updateEntity(entity, type, context));
        } catch (Throwable t) {
            Monitoring.increment(metrics.updateCounterErrors.name(), metrics.TAGS);
            throw new DataStoreException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> E updateEntity(@Nonnull E entity, @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public <E extends IEntity> boolean delete(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException {
        setTag(Metrics.METRIC_TAG_DELETE, type);
        try {
            Monitoring.increment(metrics.deleteCounter.name(), metrics.TAGS);
            return metrics.deleteLatency.record(() -> deleteEntity(key, type, context));
        } catch (Throwable t) {
            Monitoring.increment(metrics.deleteCounterErrors.name(), metrics.TAGS);
            throw new DataStoreException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> boolean deleteEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public <E extends IEntity> E find(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException {
        setTag(Metrics.METRIC_TAG_READ, type);
        try {
            Monitoring.increment(metrics.readCounter.name(), metrics.TAGS);
            return metrics.readLatency.record(() -> findEntity(key, type, context));
        } catch (Throwable t) {
            Monitoring.increment(metrics.readCounterErrors.name(), metrics.TAGS);
            throw new DataStoreException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> E findEntity(@Nonnull Object key, @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public <E extends IEntity> BaseSearchResult<E> search(@Nonnull String query,
                                                          int offset,
                                                          int maxResults,
                                                          @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException {
        setTag(Metrics.METRIC_TAG_SEARCH, type);
        try {
            Monitoring.increment(metrics.searchCounter.name(), metrics.TAGS);
            return metrics.searchLatency.record(() -> doSearch(query, offset, maxResults, type, context));
        } catch (Throwable t) {
            Monitoring.increment(metrics.searchCounterErrors.name(), metrics.TAGS);
            throw new DataStoreException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query,
                                                                     int offset,
                                                                     int maxResults,
                                                                     @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public <E extends IEntity> BaseSearchResult<E> search(@Nonnull String query,
                                                          int offset,
                                                          int maxResults,
                                                          Map<String, Object> parameters,
                                                          @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException {
        setTag(Metrics.METRIC_TAG_SEARCH, type);
        try {
            Monitoring.increment(metrics.searchCounter.name(), metrics.TAGS);
            return metrics.searchLatency.record(() -> doSearch(query, offset, maxResults, parameters, type, context));
        } catch (Throwable t) {
            Monitoring.increment(metrics.searchCounterErrors.name(), metrics.TAGS);
            throw new DataStoreException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    public abstract <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query,
                                                                     int offset,
                                                                     int maxResults,
                                                                     Map<String, Object> parameters,
                                                                     @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException;

    @SuppressWarnings("rawtypes")
    public <E extends IEntity> BaseSearchResult<E> search(@Nonnull String query, @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException {
        return search(query, 0, maxResults, type, context);
    }

    @SuppressWarnings("rawtypes")
    public <E extends IEntity> BaseSearchResult<E> search(@Nonnull String query,
                                                          Map<String, Object> parameters,
                                                          @Nonnull Class<? extends E> type, Context context) throws
            DataStoreException {
        return search(query, 0, maxResults, parameters, type, context);
    }

    @Override
    public void close() throws IOException {
        try {
            if (dataStoreManager != null) {
                dataStoreManager.close(this);
            }
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    public void configure(@Nonnull DataStoreConfig config) throws ConfigurationException {
        throw new ConfigurationException("Method not supported...");
    }

    public abstract DataStoreAuditContext context();

    private static final class Metrics {
        public static final String TAG_OPERATION = "operation";
        public static final String TAG_ENTITY = "entity";

        public static final String METRIC_TAG_READ = "READ";
        public static final String METRIC_TAG_SEARCH = "SEARCH";
        public static final String METRIC_TAG_CREATE = "CREATE";
        public static final String METRIC_TAG_UPDATE = "UPDATE";
        public static final String METRIC_TAG_DELETE = "DELETE";
        private final String METRIC_LATENCY_CREATE = String.format("%s.%s.CREATE", "%s", "%s");
        private final String METRIC_LATENCY_UPDATE = String.format("%s.%s.UPDATE", "%s", "%s");
        private final String METRIC_LATENCY_DELETE = String.format("%s.%s.DELETE", "%s", "%s");
        private final String METRIC_LATENCY_READ = String.format("%s.%s.READ", "%s", "%s");
        private final String METRIC_LATENCY_SEARCH = String.format("%s.%s.SEARCH", "%s", "%s");
        private final String METRIC_COUNTER_CREATE = String.format("%s.%s.COUNT.CREATE", "%s", "%s");
        private final String METRIC_COUNTER_UPDATE = String.format("%s.%s.COUNT.UPDATE", "%s", "%s");
        private final String METRIC_COUNTER_DELETE = String.format("%s.%s.COUNT.DELETE", "%s", "%s");
        private final String METRIC_COUNTER_READ = String.format("%s.%s.COUNT.READ", "%s", "%s");
        private final String METRIC_COUNTER_SEARCH = String.format("%s.%s.COUNT.SEARCH", "%s", "%s");
        private final String METRIC_COUNTER_ERROR_CREATE = String.format("%s.%s.COUNT.ERRORS.CREATE", "%s", "%s");
        private final String METRIC_COUNTER_ERROR_UPDATE = String.format("%s.%s.COUNT.ERRORS.UPDATE", "%s", "%s");
        private final String METRIC_COUNTER_ERROR_DELETE = String.format("%s.%s.COUNT.ERRORS.DELETE", "%s", "%s");
        private final String METRIC_COUNTER_READ_ERROR = String.format("%s.%s.COUNT.ERRORS.READ", "%s", "%s");
        private final String METRIC_COUNTER_SEARCH_ERROR = String.format("%s.%s.COUNT.ERRORS.SEARCH", "%s", "%s");
        public KeyValuePair<String, String>[] TAGS = new KeyValuePair[2];
        /**
         * Metrics - CRUD Latency
         */
        @Setter(AccessLevel.NONE)
        protected Timer createLatency = null;
        /**
         * Metrics - CRUD Latency
         */
        @Setter(AccessLevel.NONE)
        protected Timer updateLatency = null;
        /**
         * Metrics - CRUD Latency
         */
        @Setter(AccessLevel.NONE)
        protected Timer deleteLatency = null;
        /**
         * Metrics - Read Latency
         */
        @Setter(AccessLevel.NONE)
        protected Timer readLatency = null;
        /**
         * Metrics - Read Latency
         */
        @Setter(AccessLevel.NONE)
        protected Timer searchLatency = null;
        /**
         * Counter - CRUD events
         */
        @Setter(AccessLevel.NONE)
        protected Id createCounter = null;
        /**
         * Counter - CRUD events
         */
        @Setter(AccessLevel.NONE)
        protected Id updateCounter = null;
        /**
         * Counter - CRUD events
         */
        @Setter(AccessLevel.NONE)
        protected Id deleteCounter = null;
        /**
         * Counter - Read events
         */
        @Setter(AccessLevel.NONE)
        protected Id readCounter = null;
        /**
         * Counter - Search events
         */
        @Setter(AccessLevel.NONE)
        protected Id searchCounter = null;
        /**
         * Counter - CRUD Error events
         */
        @Setter(AccessLevel.NONE)
        protected Id createCounterErrors = null;
        /**
         * Counter - CRUD Error events
         */
        @Setter(AccessLevel.NONE)
        protected Id updateCounterErrors = null;
        /**
         * Counter - CRUD Error events
         */
        @Setter(AccessLevel.NONE)
        protected Id deleteCounterErrors = null;
        /**
         * Counter - Read Error events
         */
        @Setter(AccessLevel.NONE)
        protected Id readCounterErrors = null;
        /**
         * Counter - Search Error events
         */
        @Setter(AccessLevel.NONE)
        protected Id searchCounterErrors = null;
    }
}
