CREATE TABLE `redismq_message` (
                                   `id` varchar(50) NOT NULL DEFAULT '' COMMENT '消息id',
                                   `queue` varchar(255) NOT NULL DEFAULT '' COMMENT '队列',
                                   `tag` varchar(255) NOT NULL DEFAULT '' COMMENT '标签',
                                   `key` varchar(255) NOT NULL DEFAULT '' COMMENT '消息唯一id用来hash均衡',
                                   `body` varchar(2500) NOT NULL DEFAULT '' COMMENT '消息体',
                                   `virtual_queue_name` varchar(255) NOT NULL DEFAULT '' COMMENT '虚拟队列',
                                   `offset` bigint(0) NOT NULL DEFAULT 0 COMMENT '偏移量',
                                   `header` varchar(255) NOT NULL DEFAULT '' COMMENT '头',
                                   `status` tinyint(0) NOT NULL DEFAULT 0 COMMENT '消息状态',
                                   `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                                   `update_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT '更新时间',
                                   PRIMARY KEY (`id`),
                                   INDEX `idx_queue`(`queue`) USING BTREE,
                                   INDEX `idx_offset`(`offset`) USING BTREE,
                                   INDEX `idx_create_time`(`create_time`) USING BTREE
) COMMENT = 'redismq消息表';