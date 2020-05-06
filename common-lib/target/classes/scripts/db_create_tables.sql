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

DROP TABLE IF EXISTS `sys_key_vault`;

CREATE TABLE `sys_key_vault`
(
    `key`            varchar(256)   NOT NULL,
    `data`           mediumblob     NOT NULL,
    `created_by`     varchar(128)   NOT NULL,
    `created_at`     decimal(24, 0) NOT NULL,
    `updated_by`     varchar(128)   NOT NULL,
    `updated_at`     decimal(24, 0) NOT NULL,
    `record_version` decimal(24, 0) NOT NULL,
    PRIMARY KEY (`key`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Table used to store encrypted keys.';

DROP TABLE IF EXISTS `sys_db_locks`;

CREATE TABLE `sys_db_locks`
(
    `namespace`       varchar(128) NOT NULL,
    `name`            varchar(128) NOT NULL,
    `locked`          tinyint(1)   NOT NULL,
    `instance_id`     varchar(128)   DEFAULT NULL,
    `timestamp`       decimal(24, 0) DEFAULT NULL,
    `read_lock_count` int(11)        DEFAULT NULL,
    `record_version`  decimal(24, 0) DEFAULT NULL,
    PRIMARY KEY (`namespace`, `name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Table used for database distributed locks.';

DROP TABLE IF EXISTS `sys_audit_records`;

CREATE TABLE `sys_audit_records`
(
    `data_store_type` varchar(128)   NOT NULL,
    `data_store_name` varchar(128)   NOT NULL,
    `record_type`     varchar(128)   NOT NULL,
    `record_id`       varchar(128)   NOT NULL,
    `audit_type`      varchar(32)    NOT NULL,
    `entity_id`       varchar(512)   NOT NULL,
    `entity_data`     longblob   DEFAULT NULL,
    `user_id`         varchar(128)   NOT NULL,
    `timestamp`       decimal(24, 0) NOT NULL,
    `change_delta`    longblob   DEFAULT NULL,
    `change_context`  mediumblob DEFAULT NULL,
    PRIMARY KEY (`data_store_type`, `data_store_name`, `record_type`, `record_id`),
    KEY `sys_audit_records_record_type_IDX` (`record_type`, `entity_id`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Data base table to persist audit log records';

DROP TABLE IF EXISTS `sys_job_audit`;

CREATE TABLE `sys_job_audit`
(
    `job_id`          varchar(128)   NOT NULL,
    `namespace`       varchar(128)   NOT NULL,
    `name`            varchar(128)   NOT NULL,
    `job_type`        varchar(256)   NOT NULL,
    `start_timestamp` decimal(24, 0) NOT NULL,
    `end_timestamp`   decimal(24, 0) DEFAULT NULL,
    `context_json`    mediumblob     NOT NULL,
    `response_json`   longblob       DEFAULT NULL,
    `error`           varchar(256)   DEFAULT NULL,
    `error_trace`     longtext       DEFAULT NULL,
    PRIMARY KEY (`job_id`),
    KEY `sys_job_audit_namespace_IDX` (`namespace`, `name`, `job_id`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Audit table for logging Scheduled Job execution history.';

DROP TABLE IF EXISTS `config_filesystem_aws`;

CREATE TABLE `config_filesystem_aws`
(
    `name`            varchar(256) NOT NULL,
    `region`          varchar(118) NOT NULL,
    `profile`         varchar(128) NOT NULL,
    `connection_type` varchar(512) NOT NULL,
    `register_types`  text DEFAULT NULL,
    `parameters`      text DEFAULT NULL,
    PRIMARY KEY (`name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Table to store AWS S3 connection definitions.';

DROP TABLE IF EXISTS `config_ds_filesystem_aws`;

CREATE TABLE `config_ds_filesystem_aws`
(
    `name`                        varchar(256) NOT NULL,
    `data_store_class`            varchar(512) NOT NULL,
    `description`                 varchar(1024) DEFAULT NULL,
    `connection`                  varchar(256) NOT NULL,
    `connection_type`             varchar(512) NOT NULL,
    `audited`                     tinyint(1)    DEFAULT 0,
    `audit_logger_name`           varchar(256)  DEFAULT NULL,
    `audit_context_provider_type` varchar(512)  DEFAULT NULL,
    `bucket`                      varchar(256) NOT NULL,
    `temp_directory`              varchar(2048) DEFAULT NULL,
    `user_cache`                  tinyint(1)    DEFAULT 0,
    `max_cache_size`              int(11)       DEFAULT NULL,
    `cache_expiry_window`         int(11)       DEFAULT NULL,
    PRIMARY KEY (`name`),
    KEY `config_ds_filesystem_aws_data_store_class_IDX` (`data_store_class`, `name`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Table to save/load AWS S3 data store configuration.';