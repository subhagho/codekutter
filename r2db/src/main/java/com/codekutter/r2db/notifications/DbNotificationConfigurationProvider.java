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

package com.codekutter.r2db.notifications;

import com.codekutter.common.model.ID;
import com.codekutter.common.stores.BaseSearchResult;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.impl.EntitySearchResult;
import com.codekutter.common.stores.impl.RdbmsDataStore;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.r2db.driver.EntityManager;
import com.codekutter.r2db.notifications.model.Subscription;
import com.codekutter.r2db.notifications.model.SubscriptionId;
import com.codekutter.r2db.notifications.model.Topic;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.R2dbEnv;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "configuration-provider")
public class DbNotificationConfigurationProvider implements INotificationConfigurationProvider {
    @ConfigAttribute(name = "dataStore", required = true)
    private String dataStoreName;

    /**
     * Fetch all the topics for the given shard (if shard < 0, fetch all).
     *
     * @return - List of Topics
     * @throws NotificationException
     */
    @Override
    public List<Topic> fetchTopics() throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                String qstr = null;
                Map<String, Object> params = new HashMap<>();
                qstr = String.format("FROM %s WHERE active = :is_active AND deleted = :is_deleted",
                        Topic.class.getCanonicalName());
                params.put("is_active", true);
                params.put("is_deleted", false);
                BaseSearchResult<Topic> topics = dataStore.search(qstr, 0, -1, params, Topic.class, null);
                if (topics instanceof EntitySearchResult) {
                    EntitySearchResult<Topic> result = (EntitySearchResult<Topic>) topics;
                    if (result.getEntities() != null && !result.getEntities().isEmpty()) {
                        return new ArrayList<>(result.getEntities());
                    }
                }
            }
            return null;
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    private RdbmsDataStore getDataStore() throws Exception {
        EntityManager entityManager = R2dbEnv.env().getEntityManager();
        if (entityManager == null) throw new DataStoreException("Entity Manager instance is null...");
        RdbmsDataStore dataStore = (RdbmsDataStore) entityManager.dataStoreManager().getDataStore(dataStoreName, RdbmsDataStore.class);
        if (dataStore == null) {
            throw new DataStoreException(String.format("Specified data store not found. [name=%s]", dataStoreName));
        }
        return dataStore;
    }

    /**
     * Find the topic specified by the Topic ID.
     *
     * @param topicId - Topic ID to search
     * @return - Fetched Topic
     * @throws NotificationException
     */
    @Override
    public Topic findTopic(@Nonnull String topicId) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                return dataStore.find(new ID(topicId), Topic.class, null);
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    /**
     * Find the topic definition by topic name.
     *
     * @param name - Topic Name
     * @return - Fetched Topic
     * @throws NotificationException
     */
    @Override
    public Topic findTopicByName(@Nonnull String name) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                String qstr = String.format("FROM %s WHERE name = :name", Topic.class.getCanonicalName());
                Map<String, Object> params = new HashMap<>();
                params.put("name", name);
                BaseSearchResult<Topic> topics = dataStore.search(qstr, 0, -1, params, Topic.class, null);
                if (topics instanceof EntitySearchResult) {
                    EntitySearchResult<Topic> result = (EntitySearchResult<Topic>) topics;
                    if (result.getEntities() != null && !result.getEntities().isEmpty()) {
                        for (Topic t : result.getEntities()) {
                            return t;
                        }
                    }
                }
            }
            return null;
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    /**
     * Create a new Topic configuration.
     *
     * @param topic - Topic Configuration
     * @return - Topic instance.
     * @throws NotificationException
     */
    @Override
    public Topic createTopic(@Nonnull Topic topic) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                topic = dataStore.create(topic, Topic.class, null);
                dataStore.commit();
                return topic;
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    /**
     * Update the passed topic instance.
     *
     * @param topic - Topic instance
     * @return - Updated Topic instance
     * @throws NotificationException
     */
    @Override
    public Topic updateTopic(@Nonnull Topic topic) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                topic = dataStore.update(topic, Topic.class, null);
                dataStore.commit();
                return topic;
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    /**
     * Delete the specified topic. If hard delete will delete the record,
     * else will mark as deleted.
     *
     * @param topicId    - Topic ID to delete.
     * @param hardDelete - Do a hard delete.
     * @return
     * @throws NotificationException
     */
    @Override
    public boolean deleteTopic(@Nonnull String topicId, boolean hardDelete) throws NotificationException {
        try {
            boolean ret = false;
            try (RdbmsDataStore dataStore = getDataStore()) {
                Topic t = dataStore.find(new ID(topicId), Topic.class, null);
                if (t != null) {
                    if (hardDelete) {
                        ret = dataStore.delete(new ID(topicId), Topic.class, null);
                        if (ret) {
                            deleteSubscriptions(t.getId().getId(), true, dataStore);
                        }
                    } else {
                        t.setActive(false);
                        t.setDeleted(true);
                        t.setUpdatedDate(System.currentTimeMillis());

                        t = dataStore.update(t, Topic.class, null);
                        if (t != null) {
                            int count = deleteSubscriptions(t.getId().getId(), false, dataStore);
                            LogUtils.debug(getClass(),
                                    String.format("Deleted [%d] subscriptions for topic [%s]", count, t.getName()));
                            ret = true;
                        }
                    }
                    dataStore.commit();
                }
            }
            return ret;
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    /**
     * Fetch all active subscriptions.
     *
     * @return - List of active subscriptions.
     * @throws NotificationException
     */
    @Override
    public List<Subscription> fetchSubscriptions() throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                String qstr = String.format("FROM %s WHERE active = :active AND deleted = :deleted",
                        Subscription.class.getCanonicalName());
                Map<String, Object> params = new HashMap<>();
                params.put("active", true);
                params.put("deleted", false);
                BaseSearchResult<Subscription> topics
                        = dataStore.search(qstr, 0, -1, params, Subscription.class, null);
                if (topics instanceof EntitySearchResult) {
                    EntitySearchResult<Subscription> result = (EntitySearchResult<Subscription>) topics;
                    if (result.getEntities() != null && !result.getEntities().isEmpty()) {
                        return new ArrayList<>(result.getEntities());
                    }
                }
                return null;
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    /**
     * Find all active subscriptions for the specified topic.
     *
     * @param topicId - Topic ID
     * @return - List of fetched subscriptions.
     * @throws NotificationException
     */
    @Override
    public List<Subscription> findSubscriptions(@Nonnull String topicId, int shardId, int numShards) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                return findSubscriptions(topicId, shardId, numShards, dataStore);
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    private List<Subscription> findSubscriptions(@Nonnull String topicId, int shardId, int numShards, RdbmsDataStore dataStore) throws DataStoreException {
        String qstr = String.format("FROM %s WHERE id.topicId = :topic_id and MOD(hashValue, :num_shards) = :shard_id " +
                        "AND active = :active AND deleted = :deleted",
                Subscription.class.getCanonicalName());
        Map<String, Object> params = new HashMap<>();
        params.put("topic_id", topicId);
        params.put("num_shards", numShards);
        params.put("shard_id", shardId);
        params.put("active", true);
        params.put("deleted", false);
        BaseSearchResult<Subscription> topics
                = dataStore.search(qstr, 0, -1, params, Subscription.class, null);
        if (topics instanceof EntitySearchResult) {
            EntitySearchResult<Subscription> result = (EntitySearchResult<Subscription>) topics;
            if (result.getEntities() != null && !result.getEntities().isEmpty()) {
                return new ArrayList<>(result.getEntities());
            }
        }
        return null;
    }

    /**
     * Find all active subscriptions for the specified user.
     *
     * @param owner - User to fetch subscriptions for.
     * @return - List of fetched subscriptions.
     * @throws NotificationException
     */
    @Override
    public List<Subscription> findSubscriptions(@Nonnull Principal owner) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                return findSubscriptions(owner, dataStore);
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    private List<Subscription> findSubscriptions(@Nonnull Principal owner, RdbmsDataStore dataStore) throws DataStoreException {
        String qstr = String.format("FROM %s WHERE id.principal = :principal", Subscription.class.getCanonicalName());
        Map<String, Object> params = new HashMap<>();
        params.put("principal", owner.getName());
        BaseSearchResult<Subscription> topics
                = dataStore.search(qstr, 0, -1, params, Subscription.class, null);
        if (topics instanceof EntitySearchResult) {
            EntitySearchResult<Subscription> result = (EntitySearchResult<Subscription>) topics;
            if (result.getEntities() != null && !result.getEntities().isEmpty()) {
                return new ArrayList<>(result.getEntities());
            }
        }
        return null;
    }

    /**
     * Find the subscription for the specified ID.
     *
     * @param id - Subscriptions ID
     * @return
     * @throws NotificationException
     */
    @Override
    public Subscription findSubscription(@Nonnull SubscriptionId id) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                return dataStore.find(id, Subscription.class, null);
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    /**
     * Create a new subscription.
     *
     * @param subscription - Subscription instance
     * @return - Created subscription instance.
     * @throws NotificationException
     */
    @Override
    public Subscription createSubscription(@Nonnull Subscription subscription) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                subscription = dataStore.create(subscription, Subscription.class, null);
                if (subscription != null) {
                    dataStore.commit();
                }
                return subscription;
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    /**
     * Update the specified subscription.
     *
     * @param subscription - Subscription instance to update.
     * @return - Updated subscription instance.
     * @throws NotificationException
     */
    @Override
    public Subscription updateSubscription(@Nonnull Subscription subscription) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                subscription = dataStore.update(subscription, Subscription.class, null);
                if (subscription != null) {
                    dataStore.commit();
                }
                return subscription;
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    /**
     * Delete the specified subscription.
     *
     * @param id - Subscription ID to delete
     * @return - Is deleted?
     * @throws NotificationException
     */
    @Override
    public boolean deleteSubscription(@Nonnull SubscriptionId id, boolean hardDelete) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                boolean ret = deleteSubscription(id, hardDelete, dataStore);
                if (ret) {
                    dataStore.commit();
                }
                return ret;
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    private boolean deleteSubscription(@Nonnull SubscriptionId id, boolean hardDelete, RdbmsDataStore dataStore) throws DataStoreException {
        boolean ret = false;
        if (hardDelete) {
            ret = dataStore.delete(id, Subscription.class, null);
        } else {
            Subscription subscription = dataStore.find(id, Subscription.class, null);
            if (subscription != null) {
                subscription.setActive(false);
                subscription.setDeleted(true);
                subscription.setUpdatedDate(System.currentTimeMillis());
                subscription = dataStore.update(subscription, Subscription.class, null);
                if (subscription != null) {
                    ret = true;
                }
            }
        }
        return ret;
    }

    /**
     * Delete all subscriptions for the specified topic.
     *
     * @param topicId - Topic ID to delete subscriptions for.
     * @return - # of subscription entries deleted.
     * @throws NotificationException
     */
    @Override
    public int deleteSubscriptions(@Nonnull String topicId, boolean hardDelete) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                int count = deleteSubscriptions(topicId, hardDelete, dataStore);
                if (count > 0) {
                    dataStore.commit();
                }
                return count;
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }


    private int deleteSubscriptions(@Nonnull String topicId, boolean hardDelete, RdbmsDataStore dataStore) throws DataStoreException {
        int count = 0;
        // Get all the subscriptions to be deleted.
        List<Subscription> subscriptions = findSubscriptions(topicId, 0, 1, dataStore);
        if (subscriptions != null && !subscriptions.isEmpty()) {
            for (Subscription subscription : subscriptions) {
                if (deleteSubscription(subscription.getId(), hardDelete, dataStore)) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Delete all subscriptions for the specified owner.
     *
     * @param owner - Subscription Owner
     * @return - # of subscriptions entries deleted.
     * @throws NotificationException
     */
    @Override
    public int deleteSubscriptions(@Nonnull Principal owner, boolean hardDelete) throws NotificationException {
        try {
            try (RdbmsDataStore dataStore = getDataStore()) {
                int count = deleteSubscriptions(owner, hardDelete, dataStore);
                if (count > 0) {
                    dataStore.commit();
                }
                return count;
            }
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    private int deleteSubscriptions(@Nonnull Principal owner, boolean hardDelete, RdbmsDataStore dataStore) throws NotificationException, DataStoreException {
        int count = 0;
        List<Subscription> subscriptions = findSubscriptions(owner, dataStore);
        if (subscriptions != null && !subscriptions.isEmpty()) {
            for (Subscription subscription : subscriptions) {
                if (deleteSubscription(subscription.getId(), hardDelete, dataStore)) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
    }

    @Override
    public void close() throws IOException {

    }
}
