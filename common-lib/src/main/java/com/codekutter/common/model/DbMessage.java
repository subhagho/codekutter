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

package com.codekutter.common.model;

import com.codekutter.common.messaging.ESendState;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;

@Getter
@Setter
@Entity
@Table(name = "tb_message_cache")
public class DbMessage {
    @Column(name = "queue_name")
    private String queue;
    @Id
    @Column(name = "id")
    private String messageId;
    @Column(name = "jms_message_id")
    private String jmsMessageId;
    @Column(name = "checksum")
    private String checksum;
    @Column(name = "length")
    private int length;
    @Column(name = "created_timestamp")
    private long createdTimestamp;
    @Column(name = "sent_timestamp")
    private long sentTimestamp;
    @Enumerated(EnumType.STRING)
    @Column(name = "state")
    private ESendState state;
    @Column(name = "body")
    private byte[] body;
    @Column(name = "sender")
    private String sender;
    @Column(name = "error")
    private String error;
    @Column(name = "read_instance_id")
    private String instanceId;
}
