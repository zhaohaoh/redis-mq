package com.redismq.common.constant;

import com.redismq.common.config.GlobalConfigCache;
import org.apache.commons.lang3.StringUtils;

public class RedisMQConstant {

    public static String NAMESPACE;

    public static final String DEAD_NAME = "DEAD";

    public static final String PREFIX = "REDISMQ_";

    public static final String TOPIC = "PUSH_TOPIC";

    public static final String SERVERS = "SERVERS_TOPIC";

    public static final String REBALANCE_TOPIC = "REBALANCE_TOPIC";

    public static final String CLIENTS_KEY = "CLIENTS";

    public static final String SERVER = "SERVER";

    public static final String REDIS_MQ_SEND_MSG_INCREMENT = "INCRMENT";

    public static final String REDISMQ_REBALANCE_LOCK = "REBALANCE_LOCK";

    public static final String VIRTUAL_LOCK = "VIRTUAL_LOCK";

    private RedisMQConstant() {
    }

    @Deprecated
    public static String getQueueCollection() {
        return RedisMqKeys.queueCollection();
    }

    @Deprecated
    public static String getGroupCollection() {
        return RedisMqKeys.groupCollection();
    }

    @Deprecated
    public static String getOffsetGroupCollection(String group) {
        return RedisMqKeys.groupVqueues(group);
    }

    @Deprecated
    public static String getQueueOffsetKey(String queue) {
        return RedisMqKeys.queueOffset(queue);
    }

    @Deprecated
    public static String getVqueueOffsetKey(String vqueue) {
        return RedisMqKeys.vqueueOffset(vqueue);
    }

    @Deprecated
    public static String getClientCollection() {
        return RedisMqKeys.clientCollection();
    }

    @Deprecated
    public static String getServerCollection() {
        return RedisMqKeys.serverCollection();
    }

    @Deprecated
    public static String getVQueueNameByVQueue(String queue) {
        return RedisMqKeys.vqueueRoot(queue);
    }

    @Deprecated
    public static String getDeadQueueNameByQueue(String queue) {
        return RedisMqKeys.groupDead(queue, currentGroupId());
    }

    @Deprecated
    public static String getDeadBodyKeyByQueue(String queue) {
        return RedisMqKeys.groupDeadBody(queue, currentGroupId());
    }

    @Deprecated
    public static String getTopic() {
        return RedisMqKeys.pullTopic();
    }

    @Deprecated
    public static String getServerTopic() {
        return RedisMqKeys.serverTopic();
    }

    @Deprecated
    public static String getRebalanceTopic(String groupId) {
        return RedisMqKeys.rebalanceTopic(groupId);
    }

    public static String getQueueNameByVirtual(String virtual) {
        return RedisMqKeys.extractQueueName(virtual);
    }

    @Deprecated
    public static String getRebalanceLock() {
        return RedisMqKeys.rebalanceLock();
    }

    @Deprecated
    public static String getSendIncrement() {
        return RedisMqKeys.sendSequence();
    }

    @Deprecated
    public static String getVirtualQueueLock(String virtualQueueName) {
        return RedisMqKeys.vqueueLock(virtualQueueName);
    }

    @Deprecated
    public static String getWorkIdZset() {
        return RedisMqKeys.workerIds();
    }

    @Deprecated
    public static String getWorkIdLock() {
        return RedisMqKeys.workerIdLock();
    }

    @Deprecated
    public static String getGroupVqueuesKey(String groupId) {
        return RedisMqKeys.groupVqueues(groupId);
    }

    @Deprecated
    public static String getGroupMessageKey(String vqueue, String groupId) {
        return RedisMqKeys.groupMessages(vqueue, groupId);
    }

    @Deprecated
    public static String getGroupOffsetKey(String vqueue, String groupId) {
        return RedisMqKeys.groupOffset(vqueue, groupId);
    }

    private static String currentGroupId() {
        String groupId = null;
        if (GlobalConfigCache.CONSUMER_CONFIG != null) {
            groupId = GlobalConfigCache.CONSUMER_CONFIG.getGroupId();
        }
        if (StringUtils.isBlank(groupId)) {
            throw new IllegalStateException("RedisMQ groupId 未初始化");
        }
        return groupId;
    }
}
