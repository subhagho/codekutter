DROP TABLE IF EXISTS `tb_key_vault`;

CREATE TABLE `tb_key_vault`
(
    `key`            varchar(256)   NOT NULL,
    `data`           blob           NOT NULL,
    `created_by`     varchar(128)   NOT NULL,
    `created_at`     decimal(24, 0) NOT NULL,
    `updated_by`     varchar(128)   NOT NULL,
    `updated_at`     decimal(24, 0) NOT NULL,
    `record_version` decimal(24, 0) NOT NULL,
    PRIMARY KEY (`key`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Table used to store encrypted keys.';

DROP TABLE IF EXISTS `tb_db_locks`;

CREATE TABLE `tb_db_locks`
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
