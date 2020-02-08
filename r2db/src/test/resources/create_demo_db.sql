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

drop table if exists tb_customer;

create table tb_customer
(
	customer_id varchar(128) not null
		primary key,
	first_name varchar(100) not null,
	last_name varchar(100) not null,
	date_of_birth datetime not null,
	email_id varchar(100) not null,
	phone_no varchar(16) not null
)
comment 'Demo customer entity table.' charset=utf8;

drop table if exists tb_orders;

CREATE TABLE `tb_orders` (
  `order_id` varchar(128) NOT NULL,
  `customer_id` varchar(128) NOT NULL,
  `order_amount` decimal(12,2) NOT NULL DEFAULT 0.00,
  `created_date` datetime NOT NULL,
  PRIMARY KEY (`order_id`),
  KEY `tb_orders_fk` (`customer_id`),
  CONSTRAINT `tb_orders_fk` FOREIGN KEY (`customer_id`) REFERENCES `tb_customer` (`customer_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Demo orders table.';

drop table if exists tb_product;

create table tb_product
(
	product_id varchar(128) not null
		primary key,
	product_name varchar(128) not null,
	description text null,
	base_price decimal(24,2) not null,
	created_date decimal(24) not null
)
comment 'Demo product table.' charset=utf8;

drop table if exists tb_order_items;

create table tb_order_items
(
	order_id varchar(128) not null,
	product_id varchar(128) not null,
	quantity decimal(12,2) not null,
	unit_price decimal(12,2) not null,
	primary key (order_id, product_id),
	constraint tb_order_items_product_fk
		foreign key (product_id) references tb_product (product_id)
			on update cascade on delete cascade
)
comment 'Demo order items table' charset=utf8;

