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

import com.codekutter.r2db.notifications.model.Notification;
import com.codekutter.r2db.notifications.model.NotificationQuery;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.security.Principal;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class ChronicleNotificationHandler extends AbstractNotificationHandler {
    @ConfigAttribute
    private int numShards = -1;
    @ConfigAttribute
    private int shardId = -1;

    @Override
    public Notification doSend(@Nonnull String topic, @Nonnull Principal sender, @Nonnull Notification notification) throws NotificationException {
        return null;
    }

    @Override
    public List<Notification> receive(@Nonnull String topic, @Nonnull Principal receiver, @Nonnull NotificationQuery query) throws NotificationException {
        return null;
    }

    @Override
    public Notification find(@Nonnull String topic, @Nonnull String notificationId, @Nonnull Principal receiver) throws NotificationException {
        return null;
    }

    @Override
    public Notification doMarkAsRead(@Nonnull String topic, @Nonnull String notificationId, @Nonnull Principal receiver) throws NotificationException {
        return null;
    }

    @Override
    public Notification doMarkAsDismissed(@Nonnull String topic, @Nonnull String notificationId, @Nonnull Principal receiver) throws NotificationException {
        return null;
    }

    @Override
    public Notification doDelete(@Nonnull String topic, @Nonnull String notificationId, @Nonnull Principal receiver) throws NotificationException {
        return null;
    }

    @Override
    public void doConfigure(@Nonnull ConfigPathNode node) throws ConfigurationException {

    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {

    }

    @Override
    public void close() throws IOException {

    }
}
