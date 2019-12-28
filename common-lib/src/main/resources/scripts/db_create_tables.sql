DROP TABLE IF EXISTS `tb_key_vault`;

CREATE TABLE `tb_key_vault` (
  `key` varchar(256) NOT NULL,
  `data` blob NOT NULL,
  `created_by` varchar(128) NOT NULL,
  `created_at` decimal(24,0) NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` decimal(24,0) NOT NULL,
  `record_version` decimal(24,0) NOT NULL,
  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;