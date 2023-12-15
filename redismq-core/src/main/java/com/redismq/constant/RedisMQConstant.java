package com.redismq.constant;

/**
 * @Author: hzh
 * @Date: 2022/11/4 15:19 全局常量
 */

import org.apache.commons.lang3.StringUtils;

import static com.redismq.constant.GlobalConstant.SPLITE;

public class RedisMQConstant {
    
    
    /**
     * 分组
     */
    public static String NAMESPACE;
    
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
    
    /**
     * 获取队列集合
     *
     * @return {@link String}
     */
    public static String getQueueCollection() {
        return PREFIX + getTopic() + SPLITE + "QUEUE";
    }
    
    /**
     * 获取客户端集合
     *
     * @return {@link String}
     */
    public static String getClientCollection() {
        return PREFIX + NAMESPACE + SPLITE + CLIENTS_KEY;
    }
    
    public static String getQueueNameByQueue(String queue) {
        if (!StringUtils.startsWith(queue, PREFIX + NAMESPACE + SPLITE)) {
            return PREFIX + NAMESPACE + SPLITE + queue;
        }
        return queue;
    }
    
    public static String getDeadQueueNameByQueue(String queue) {
        if (!StringUtils.startsWith(queue, PREFIX + NAMESPACE + DEAD_NAME + SPLITE)) {
            return PREFIX + NAMESPACE + DEAD_NAME + SPLITE + queue;
        }
        return PREFIX + NAMESPACE + DEAD_NAME + SPLITE + queue;
    }
    
    public static String getTopic() {
        return PREFIX + NAMESPACE + SPLITE + TOPIC;
    }
    
    public static String getRebalanceTopic() {
        return PREFIX + NAMESPACE + SPLITE + REBALANCE_TOPIC;
    }
    
    /**
     * 获取队列名字根据虚拟
     *
     * @param virtual 虚拟
     * @return {@link String}
     */
    public static String getQueueNameByVirtual(String virtual) {
        return StringUtils.substringBeforeLast(virtual, SPLITE);
    }
    
    public static String getRebalanceLock() {
        return PREFIX + NAMESPACE + SPLITE + REDISMQ_REBALANCE_LOCK;
    }
    
    public static String getSendIncrement() {
        return PREFIX + NAMESPACE + SPLITE + REDIS_MQ_SEND_MSG_INCREMENT;
    }
    
    public static String getVirtualQueueLock(String virtualQueueName) {
        return PREFIX + NAMESPACE + SPLITE + VIRTUAL_LOCK + SPLITE + virtualQueueName;
    }
}
