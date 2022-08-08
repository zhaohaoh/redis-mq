package com.redismq.constant;


public class RedisMQConstant {
    // 内部发布订阅消息的topic
    public static final String TOPIC = "redis-mq-push";
    // 内部负载均衡消息的topic
    public static final String REBALANCE_TOPIC = "REBALANCE_TOPIC";

    // 内部发消息自增的key
    public static final String REDIS_MQ_SEND_MSG_INCREMENT = "REDIS_MQ_SEND_MSG_INCREMENT";
}
