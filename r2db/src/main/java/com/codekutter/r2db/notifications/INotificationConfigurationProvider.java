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

import com.codekutter.r2db.notifications.model.Subscription;
import com.codekutter.r2db.notifications.model.SubscriptionId;
import com.codekutter.r2db.notifications.model.Topic;
import com.codekutter.zconfig.common.IConfigurable;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.security.Principal;
import java.util.List;

/**
 * Interface definition for SCRUD operations on notification configurations.
 */
public interface INotificationConfigurationProvider extends IConfigurable, Closeable {
    /**
     * Fetch all the topics for the given shard (if shard < 0, fetch all).
     *
     * @return - List of Topics
     * @throws NotificationException
     */
    List<Topic> fetchTopics() throws NotificationException;

    /**
     * Find the topic specified by the Topic ID.
     *
     * @param topicId - Topic ID to search
     * @return - Fetched Topic
     * @throws NotificationException
     */
    Topic findTopic(@Nonnull String topicId) throws NotificationException;

    /**
     * Find the topic definition by topic name.
     *
     * @param name - Topic Name
     * @return - Fetched Topic
     * @throws NotificationException
     */
    Topic findTopicByName(@Nonnull String name) throws NotificationException;

    /**
     * Create a new Topic configuration.
     *
     * @param topic - Topic Configuration
     * @return - Topic instance.
     * @throws NotificationException
     */
    Topic createTopic(@Nonnull Topic topic) throws NotificationException;

    /**
     * Update the passed topic instance.
     *
     * @param topic - Topic instance
     * @return - Updated Topic instance
     * @throws NotificationException
     */
    Topic updateTopic(@Nonnull Topic topic) throws NotificationException;

    /**
     * Delete the specified topic. If hard delete will delete the record,
     * else will mark as deleted.
     *
     * @param topicId - Topic ID to delete.
     * @param hardDelete - Do a hard delete.
     * @return
     * @throws NotificationException
     */
    boolean deleteTopic(@Nonnull String topicId, boolean hardDelete) throws NotificationException;

    /**
     * Find all active subscriptions for the specified topic.
     *
     * @param topicId - Topic ID
     * @return - List of fetched subscriptions.
     * @throws NotificationException
     */
    List<Subscription> findSubscriptions(@Nonnull String topicId, int shardId, int numShards) throws NotificationException;

    /**
     * Find all active subscriptions for the specified user.
     *
     * @param owner - User to fetch subscriptions for.
     * @return - List of fetched subscriptions.
     * @throws NotificationException
     */
    List<Subscription> findSubscriptions(@Nonnull Principal owner) throws NotificationException;

    /**
     * Find the subscription for the specified ID.
     *
     * @param id - Subscriptions ID
     * @return
     * @throws NotificationException
     */
    Subscription findSubscription(@Nonnull SubscriptionId id) throws NotificationException;

    /**
     * Create a new subscription.
     *
     * @param subscription - Subscription instance
     * @return - Created subscription instance.
     * @throws NotificationException
     */
    Subscription createSubscription(@Nonnull Subscription subscription) throws NotificationException;

    /**
     * Update the specified subscription.
     *
     * @param subscription - Subscription instance to update.
     * @return - Updated subscription instance.
     * @throws NotificationException
     */
    Subscription updateSubscription(@Nonnull Subscription subscription) throws NotificationException;

    /**
     * Delete the specified subscription.
     *
     * @param id - Subscription ID to delete
     * @return - Is deleted?
     * @throws NotificationException
     */
    boolean deleteSubscription(@Nonnull SubscriptionId id, boolean hardDelete) throws NotificationException;

    /**
     * Delete all subscriptions for the specified topic.
     *
     * @param topicId - Topic ID to delete subscriptions for.
     * @return - # of subscription entries deleted.
     * @throws NotificationException
     */
    int deleteSubscriptions(@Nonnull String topicId, boolean hardDelete) throws NotificationException;

    /**
     * Delete all subscriptions for the specified owner.
     *
     * @param owner - Subscription Owner
     * @return - # of subscriptions entries deleted.
     * @throws NotificationException
     */
    int deleteSubscriptions(@Nonnull Principal owner, boolean hardDelete) throws NotificationException;
}
