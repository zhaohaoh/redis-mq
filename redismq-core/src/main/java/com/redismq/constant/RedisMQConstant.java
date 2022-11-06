package com.redismq.constant;


/**
 * @Author: hzh
 * @Date: 2022/11/4 15:19
 * 全局常量
 */
import static com.redismq.constant.GlobalConstant.SPLITE;

public class RedisMQConstant {

    /**
     * 分组
     */
    public static String GROUP;
    /**
     * 死信队列名字
     */
    public static final String DEAD_NAME = "DEAD";
    /**
     * 前缀
     */
    public static final String PREFIX = "REDISMQ_";
    /**
     * 内部发布订阅消息的topic
     */
    public static final String TOPIC = "PUSH_TOPIC";

    /**
     * 内部负载均衡消息的topic
     */
    public static final String REBALANCE_TOPIC = "REBALANCE_TOPIC";

    /**
     * 客户端
     */
    public static final String CLIENTS_KEY = "CLIENTS";

    /**
     * 内部发消息自增的key
     */
    public static final String REDIS_MQ_SEND_MSG_INCREMENT = "INCRMENT";

    /**
     * redismq 负载均衡锁
     */
    public static final String REDISMQ_REBALANCE_LOCK = "REBALANCE_LOCK";

    /**
     * 虚拟队列锁
     */
    public static final String VIRTUAL_LOCK = "VIRTUAL_LOCK";

    public static String getQueueNameByTopic(String topic) {
        return PREFIX + GROUP + SPLITE + topic;
    }

    public static String getDeadQueueNameByTopic(String topic) {
        return PREFIX + GROUP + DEAD_NAME + SPLITE + topic;
    }

    public static String getTopic() {
        return PREFIX + GROUP + SPLITE + TOPIC;
    }

    public static String getRebalanceTopic() {
        return PREFIX + GROUP + SPLITE + REBALANCE_TOPIC;
    }

    public static String getClientKey() {
        return PREFIX + GROUP + SPLITE + CLIENTS_KEY;
    }

    public static String getRebalanceLock() {
        return PREFIX + GROUP + SPLITE + REDISMQ_REBALANCE_LOCK;
    }

    public static String getSendIncrement() {
        return PREFIX + GROUP + SPLITE + REDIS_MQ_SEND_MSG_INCREMENT;
    }

    public static String getVirtualQueueLock(String virtualQueueName) {
        return PREFIX + GROUP + SPLITE + VIRTUAL_LOCK + SPLITE + virtualQueueName;
    }
}
