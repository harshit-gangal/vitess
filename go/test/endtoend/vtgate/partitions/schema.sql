CREATE TABLE `messages` (
    `send_id` binary(16) NOT NULL,
    `subscriber_id` bigint unsigned,
    `status_id` int unsigned NOT NULL,
    `message_created_at` datetime DEFAULT CURRENT_TIMESTAMP(),
    `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    `safe_for_work` bit(1) NOT NULL DEFAULT B'1',
    PRIMARY KEY (`send_id`, `created_at`),
    KEY `messages_subscriber_id` (`subscriber_id`, `safe_for_work`),
    KEY `messages_status_id` (`status_id`, `message_created_at`, `safe_for_work`)
) ENGINE InnoDB
    PARTITION BY RANGE (to_days(`created_at`))(
        partition p1 values less than (to_days('2022-06-05')),
        partition p2 values less than (to_days('2022-06-10')),
        partition p3 values less than (to_days('2022-06-15')),
        partition p4 values less than (to_days('2022-06-20')),
        partition p5 values less than (to_days('9999-12-31')));

CREATE TABLE `send_id_idx` (
   `send_id` binary(16) NOT NULL,
   `keyspace_id` binary(8) NOT NULL,
   `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP(),
   PRIMARY KEY (`send_id`)
) ENGINE InnoDB;
