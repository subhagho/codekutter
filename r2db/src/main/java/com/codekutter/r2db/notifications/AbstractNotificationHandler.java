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

import com.codekutter.common.GlobalConstants;
import com.codekutter.common.auditing.AuditManager;
import com.codekutter.common.model.EAuditType;
import com.codekutter.common.model.EObjectState;
import com.codekutter.common.model.ObjectState;
import com.codekutter.r2db.notifications.model.*;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "notification-handler")
public abstract class AbstractNotificationHandler implements IConfigurable, Closeable {
    public static final long DEFAULT_MAX_SIZE = 1024 * 1024; // 1MB
    public static final long DEFAULT_EXPIRY_WINDOW = 30 * 24 * 60 * 60 * 1000L; // 30 days
    @ConfigAttribute(required = true)
    private String name;
    @ConfigAttribute
    private int shardId = 0;
    @ConfigAttribute
    private int replicaShardId = -1;
    @ConfigAttribute
    private int numShards = 1;
    @ConfigAttribute
    private boolean audited = false;
    @ConfigValue
    private long maxSize = DEFAULT_MAX_SIZE;
    @ConfigValue
    private long expiryWindow = DEFAULT_EXPIRY_WINDOW;
    @Setter(AccessLevel.NONE)
    private final ObjectState state = new ObjectState();
    @Setter(AccessLevel.NONE)
    private INotificationConfigurationProvider configurationProvider;
    @Setter(AccessLevel.NONE)
    private Map<String, Topic> topics = new HashMap<>();
    @Setter(AccessLevel.NONE)
    private Multimap<String, Subscription> topicSubscriptions = ArrayListMultimap.create();
    @Setter(AccessLevel.NONE)
    private Multimap<String, Subscription> subscriptions = ArrayListMultimap.create();

    public AbstractNotificationHandler withConfigurationProvider(@Nonnull INotificationConfigurationProvider configurationProvider) {
        this.configurationProvider = configurationProvider;
        return this;
    }

    public Notification send(@Nonnull String topic,
                             @Nonnull Principal sender,
                             Serializable body) throws NotificationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        try {
            Notification notification = new Notification();
            NotificationId id = new NotificationId();
            id.setTopic(topic);
            id.setId(UUID.randomUUID().toString());
            notification.setId(id);
            notification.setCreatedDate(System.currentTimeMillis());
            notification.setNotificationState(ENotificationState.New);
            notification.setPrincipal(sender.getName());
            notification.setUpdatedDate(notification.getCreatedDate());
            if (body != null) {
                String json = GlobalConstants.getJsonMapper().writeValueAsString(body);
                byte[] array = json.getBytes(GlobalConstants.defaultCharset());
                if (array != null && array.length > maxSize) {
                    throw new NotificationException(String.format("Max notification size exceeded. [max size=%d][size=%d]", maxSize, array.length));
                }
                notification.setBody(array);
            }
            notification = doSend(topic, sender, notification);
            if (audited) {
                AuditManager.get().audit(Notification.class,
                        getClass().getCanonicalName(), EAuditType.Create, notification, null, null, sender);
            }
            return notification;
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    public Notification markAsRead(@Nonnull String topic,
                                   @Nonnull String notificationId,
                                   @Nonnull Principal receiver) throws NotificationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        try {
            Notification notification = doMarkAsRead(topic, notificationId, receiver);
            if (audited) {
                AuditManager.get().audit(Notification.class,
                        getClass().getCanonicalName(), EAuditType.Update, notification, null, null, receiver);
            }
            return notification;
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    public Notification markAsDismissed(@Nonnull String topic,
                                        @Nonnull String notificationId,
                                        @Nonnull Principal receiver) throws NotificationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        try {
            Notification notification = doMarkAsDismissed(topic, notificationId, receiver);
            if (audited) {
                AuditManager.get().audit(Notification.class,
                        getClass().getCanonicalName(), EAuditType.Update, notification, null, null, receiver);
            }
            return notification;
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
    }

    public Notification delete(@Nonnull String topic,
                               @Nonnull String notificationId,
                               @Nonnull Principal receiver) throws NotificationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        try {
            Notification notification = doDelete(topic, notificationId, receiver);
            if (audited) {
                AuditManager.get().audit(Notification.class,
                        getClass().getCanonicalName(), EAuditType.Delete, notification, null, null, receiver);
            }
            return notification;
        } catch (Exception ex) {
            throw new NotificationException(ex);
        }
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
        Preconditions.checkState(configurationProvider != null);
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            if (shardId >= numShards) {
                throw new ConfigurationException(String.format("Invalid Shard configuration. [shard ID=%d][#shards=%d]", shardId, numShards));
            } else {
                shardId = 0;
                numShards = 1;
            }
            doConfigure((ConfigPathNode) node);
            state.setState(EObjectState.Available);
        } catch (Exception ex) {
            state.setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void close() throws IOException {

    }

    public abstract Notification doSend(@Nonnull String topic,
                                        @Nonnull Principal sender,
                                        @Nonnull Notification notification) throws NotificationException;

    public abstract List<Notification> receive(String topic,
                                               @Nonnull Principal receiver,
                                               @Nonnull NotificationQuery query) throws NotificationException;

    public abstract Notification find(@Nonnull String topic,
                                      @Nonnull String notificationId,
                                      @Nonnull Principal receiver) throws NotificationException;

    public abstract Notification doMarkAsRead(@Nonnull String topic,
                                              @Nonnull String notificationId,
                                              @Nonnull Principal receiver) throws NotificationException;

    public abstract Notification doMarkAsDismissed(@Nonnull String topic,
                                                   @Nonnull String notificationId,
                                                   @Nonnull Principal receiver) throws NotificationException;

    public abstract Notification doDelete(@Nonnull String topic,
                                          @Nonnull String notificationId,
                                          @Nonnull Principal receiver) throws NotificationException;

    public abstract void doConfigure(@Nonnull ConfigPathNode node) throws ConfigurationException;
}
