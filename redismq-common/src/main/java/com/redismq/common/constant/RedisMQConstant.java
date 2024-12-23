package com.redismq.common.constant;

/**
 * @Author: hzh
 * @Date: 2022/11/4 15:19 全局常量
 */

import org.apache.commons.lang3.StringUtils;

import static com.redismq.common.constant.GlobalConstant.SPLITE;
import static com.redismq.common.constant.GlobalConstant.V_QUEUE_SPLITE;

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
     * 内部发布订阅消息的topic
     */
    public static final String SERVERS = "SERVERS_TOPIC";
    
    /**
     * 内部负载均衡消息的topic
     */
    public static final String REBALANCE_TOPIC = "REBALANCE_TOPIC";
    
    /**
     * 客户端
     */
    public static final String CLIENTS_KEY = "CLIENTS";
    /**
     * 服务端
     */
    public static final String SERVER = "SERVER";
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
     * 机器id集合
     */
    private static final String WORK_ID_ZSET = "WORK_ID_ZSET";
    /**
     * 获取队列集合
     *
     * @return {@link String}
     */
    public static String getQueueCollection() {
        return getTopic() + SPLITE + "QUEUE";
    }
    
    /**
     *  Group集合
     */
    public static String getGroupCollection() {
       return PREFIX + NAMESPACE +   SPLITE + "GROUP"  ;
    }
    /**
     *  Group集合
     */
    public static String getOffsetGroupCollection(String group) {
        return PREFIX + NAMESPACE  + SPLITE + "GROUP_OFFSET:" + group;
    }
    /**
     * 获取客户端集合
     *
     * @return {@link String}
     */
    public static String getClientCollection() {
        return PREFIX + NAMESPACE +   SPLITE + CLIENTS_KEY ;
    }
    
    /**
     * 获取客户端集合
     *
     * @return {@link String}
     */
    public static String getServerCollection() {
        return PREFIX + NAMESPACE + SPLITE + SERVER;
    }
    
    public static String getVQueueNameByVQueue(String queue) {
        if (!StringUtils.startsWith(queue, PREFIX + NAMESPACE + SPLITE)) {
            return PREFIX + NAMESPACE + SPLITE + "{" + queue + "}";
        }
        return queue;
    }
    
    public static String getDeadQueueNameByQueue(String queue) {
        if (!StringUtils.startsWith(queue, PREFIX + NAMESPACE + DEAD_NAME + SPLITE)) {
            return PREFIX + NAMESPACE + DEAD_NAME + SPLITE + "{" + queue + "}";
        }
        return PREFIX + NAMESPACE + DEAD_NAME + SPLITE + "{" + queue + "}";
    }
    
    public static String getTopic() {
        return PREFIX + NAMESPACE + SPLITE + TOPIC;
    }
    
    public static String getServerTopic() {
        return PREFIX + NAMESPACE + SPLITE + SERVERS;
    }
    
    public static String getRebalanceTopic(String groupId) {
        return PREFIX + NAMESPACE + SPLITE + REBALANCE_TOPIC +SPLITE+groupId;
    }
    
    /**
     * 获取队列名字根据虚拟
     *
     * @param virtual 虚拟
     * @return {@link String}
     */
    public static String getQueueNameByVirtual(String virtual) {
        return StringUtils.substringBeforeLast(virtual, V_QUEUE_SPLITE);
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
    
    
    public static String getWorkIdZset() {
        return PREFIX + NAMESPACE + SPLITE + WORK_ID_ZSET;
    }
    
}
