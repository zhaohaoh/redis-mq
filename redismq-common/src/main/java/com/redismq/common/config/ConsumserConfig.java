package com.redismq.common.config;

import lombok.Data;

/**
 * 消费者配置
 */
@Data
public class ConsumserConfig {
    
    /**
     * 应用名
     */
    private String applicationName = "redisMQ_client";
    /**
     * 消费者组id
     */
    private String groupId ="consumerGroup";
}
