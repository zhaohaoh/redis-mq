package com.redismq.constant;


import static com.redismq.constant.QueueConstant.SPLITE;

public class RedisMQConstant {
    public static String CLUSTER;
    public static final String DEAD_NAME = "DEAD";
    public static final String PREFIX = "REDISMQ_";
    // 内部发布订阅消息的topic
    public static final String TOPIC = "PUSH_TOPIC";
    // 内部负载均衡消息的topic
    public static final String REBALANCE_TOPIC = "REBALANCE_TOPIC";

    public static final String CLIENTS_KEY = "CLIENTS";
    // 内部发消息自增的key
    public static final String REDIS_MQ_SEND_MSG_INCREMENT = "INCRMENT";

    public static final String REDISMQ_REBALANCE_LOCK = "REBALANCE_LOCK";

    public static String getQueueNameByTopic(String topic) {
        return PREFIX + CLUSTER + SPLITE + topic;
    }

    public static String getDeadQueueNameByTopic(String topic) {
        return PREFIX + CLUSTER + DEAD_NAME + SPLITE + topic;
    }

    public static String getTopic() {
        return PREFIX + CLUSTER + SPLITE + TOPIC;
    }

    public static String getRebalanceTopic() {
        return PREFIX + CLUSTER + SPLITE + REBALANCE_TOPIC;
    }

    public static String getClientKey() {
        return PREFIX + CLUSTER + SPLITE + CLIENTS_KEY;
    }

    public static String getRebalanceLock() {
        return PREFIX + CLUSTER + SPLITE + REDISMQ_REBALANCE_LOCK;
    }

    public static String getSendIncrement() {
        return PREFIX + CLUSTER + SPLITE + REDIS_MQ_SEND_MSG_INCREMENT;
    }

}
