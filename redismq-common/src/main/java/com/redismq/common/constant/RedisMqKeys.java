package com.redismq.common.constant;

import org.apache.commons.lang3.StringUtils;

import static com.redismq.common.constant.RedisMqKeySegment.BODY;
import static com.redismq.common.constant.RedisMqKeySegment.CLIENTS;
import static com.redismq.common.constant.RedisMqKeySegment.DEAD;
import static com.redismq.common.constant.RedisMqKeySegment.GROUP;
import static com.redismq.common.constant.RedisMqKeySegment.GROUPS;
import static com.redismq.common.constant.RedisMqKeySegment.LOCK;
import static com.redismq.common.constant.RedisMqKeySegment.MESSAGES;
import static com.redismq.common.constant.RedisMqKeySegment.OFFSET;
import static com.redismq.common.constant.RedisMqKeySegment.PULL;
import static com.redismq.common.constant.RedisMqKeySegment.QUEUES;
import static com.redismq.common.constant.RedisMqKeySegment.REBALANCE;
import static com.redismq.common.constant.RedisMqKeySegment.SEQUENCE;
import static com.redismq.common.constant.RedisMqKeySegment.SEND;
import static com.redismq.common.constant.RedisMqKeySegment.SERVER;
import static com.redismq.common.constant.RedisMqKeySegment.SERVERS;
import static com.redismq.common.constant.RedisMqKeySegment.TOPIC;
import static com.redismq.common.constant.RedisMqKeySegment.VQUEUES;
import static com.redismq.common.constant.RedisMqKeySegment.WORKER_IDS;

public final class RedisMqKeys {

    private static final String PREFIX = "redismq";
    private static final String SEPARATOR = ":";

    private RedisMqKeys() {
    }

    public static String queueCollection() {
        return join(PREFIX, namespace(), QUEUES.value());
    }

    public static String groupCollection() {
        return join(PREFIX, namespace(), GROUPS.value());
    }

    public static String clientCollection() {
        return join(PREFIX, namespace(), CLIENTS.value());
    }

    public static String serverCollection() {
        return join(PREFIX, namespace(), SERVERS.value());
    }

    public static String pullTopic() {
        return join(PREFIX, namespace(), TOPIC.value(), PULL.value());
    }

    public static String serverTopic() {
        return join(PREFIX, namespace(), TOPIC.value(), SERVER.value());
    }

    public static String rebalanceTopic(String groupId) {
        return join(PREFIX, namespace(), TOPIC.value(), REBALANCE.value(), requireText(groupId, "groupId"));
    }

    public static String rebalanceLock() {
        return join(PREFIX, namespace(), LOCK.value(), REBALANCE.value());
    }

    public static String sendSequence() {
        return join(PREFIX, namespace(), SEQUENCE.value(), SEND.value());
    }

    public static String workerIds() {
        return join(PREFIX, namespace(), WORKER_IDS.value());
    }

    public static String workerIdLock() {
        return join(PREFIX, namespace(), LOCK.value(), "worker-id");
    }

    public static String groupVqueues(String groupId) {
        return join(PREFIX, namespace(), GROUP.value(), requireText(groupId, "groupId"), VQUEUES.value());
    }

    public static String queueOffset(String queueName) {
        return join(PREFIX, namespace(), hashTag(extractQueueName(queueName)), OFFSET.value());
    }

    public static String vqueueRoot(String vqueue) {
        return join(PREFIX, namespace(), hashTag(extractVqueueName(vqueue)));
    }

    public static String vqueueBody(String vqueue) {
        return join(vqueueRoot(vqueue), BODY.value());
    }

    public static String vqueueOffset(String vqueue) {
        return join(vqueueRoot(vqueue), OFFSET.value());
    }

    public static String vqueueLock(String vqueue) {
        return join(vqueueRoot(vqueue), LOCK.value());
    }

    public static String vqueueGroups(String vqueue) {
        return join(vqueueRoot(vqueue), GROUPS.value());
    }

    public static String groupMessages(String vqueue, String groupId) {
        return join(vqueueRoot(vqueue), GROUP.value(), requireText(groupId, "groupId"), MESSAGES.value());
    }

    public static String groupOffset(String vqueue, String groupId) {
        return join(vqueueRoot(vqueue), GROUP.value(), requireText(groupId, "groupId"), OFFSET.value());
    }

    public static String groupDead(String vqueue, String groupId) {
        return join(vqueueRoot(vqueue), GROUP.value(), requireText(groupId, "groupId"), DEAD.value());
    }

    public static String groupDeadBody(String vqueue, String groupId) {
        return join(vqueueRoot(vqueue), GROUP.value(), requireText(groupId, "groupId"), DEAD.value(), BODY.value());
    }

    public static String extractVqueueName(String value) {
        String source = requireText(value, "value");
        String between = StringUtils.substringBetween(source, "{", "}");
        return StringUtils.isNotBlank(between) ? between : source;
    }

    public static String extractQueueName(String value) {
        String vqueue = extractVqueueName(value);
        if (!vqueue.contains(GlobalConstant.V_QUEUE_SPLITE)) {
            return vqueue;
        }
        return StringUtils.substringBeforeLast(vqueue, GlobalConstant.V_QUEUE_SPLITE);
    }

    private static String namespace() {
        return requireText(RedisMQConstant.NAMESPACE, "namespace");
    }

    private static String hashTag(String value) {
        return "{" + requireText(value, "hashTag") + "}";
    }

    private static String join(String... parts) {
        return String.join(SEPARATOR, parts);
    }

    private static String requireText(String value, String fieldName) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalStateException("RedisMQ " + fieldName + " 未初始化");
        }
        return value;
    }
}
