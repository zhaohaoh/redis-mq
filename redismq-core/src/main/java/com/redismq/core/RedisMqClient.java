package com.redismq.core;


import com.redismq.constant.PublishContant;
import com.redismq.constant.PushMessage;
import com.redismq.queue.Queue;
import com.redismq.queue.QueueManager;
import com.redismq.rebalance.ClientConfig;
import com.redismq.rebalance.RebalanceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class RedisMqClient {

    protected static final Logger log = LoggerFactory.getLogger(RedisMqClient.class);
    private final RedisListenerContainerManager redisListenerContainerManager;
    private static final String CLIENT_KEY = "REDIS_MQ_CLIENT";
    private final RedisTemplate<String, Object> redisTemplate;
    private final String clientId;
    private final RebalanceImpl rebalance;

    public RedisMqClient(RedisTemplate<String, Object> redisTemplate, RedisListenerContainerManager redisListenerContainerManager, RebalanceImpl rebalance) {
        this.redisTemplate = redisTemplate;
        this.clientId = ClientConfig.getLocalAddress();
        this.redisListenerContainerManager = redisListenerContainerManager;
        this.rebalance = rebalance;
        registerClient();
    }

    public RedisListenerContainerManager getRedisListenerContainerManager() {
        return redisListenerContainerManager;
    }


    public void registerClient() {
        //已分配的队列,真实的队列名和已分配的队列
        redisTemplate.opsForSet().add(CLIENT_KEY, clientId);
    }

    public Set<String> allClient() {
        //已分配的队列,真实的队列名和已分配的队列
        return redisTemplate.opsForSet().members(CLIENT_KEY).stream().map(Object::toString).collect(Collectors.toSet());
    }

    public void destory() {
        //已分配的队列,真实的队列名和已分配的队列
        redisTemplate.opsForSet().remove(CLIENT_KEY, clientId);
        log.info("redismq client remove");
        //停止任务
        redisListenerContainerManager.stopAll();
    }

    public void start() {
        //重新负载均衡
        rebalance.rebalance(allClient(), clientId);
        if (QueueManager.hasSubscribe()) {
            //订阅push消息
            subscribe();
            //启动队列监控
            redisListenerContainerManager.startRedisListener();
            //启动延时队列监控
            redisListenerContainerManager.startDelayRedisListener();
            repush();
        }
    }
    //启动时对任务重新进行拉取
    private void repush() {
        Map<String, List<String>> queues = QueueManager.CURRENT_VIRTUAL_QUEUES;
        queues.forEach((k, v) -> {
            Queue queue = QueueManager.getQueue(k);
            if (queue == null) {
                return;
            }
            PushMessage pushMessage = new PushMessage();
            pushMessage.setQueue(k);
            pushMessage.setTimestamp(System.currentTimeMillis());
            LinkedBlockingQueue<PushMessage> delayBlockingQueue = redisListenerContainerManager.getDelayBlockingQueue();
            LinkedBlockingQueue<String> linkedBlockingQueue = redisListenerContainerManager.getLinkedBlockingQueue();
            boolean delayState = queue.getDelayState();
            if (delayState) {
                delayBlockingQueue.add(pushMessage);
            } else {
                linkedBlockingQueue.add(k);
            }
        });
    }

    public void subscribe() {
        RedisSerializer<String> stringSerializer = redisTemplate.getStringSerializer();
        ByteArrayWrapper holder = new ByteArrayWrapper(Objects.requireNonNull(stringSerializer.serialize(PublishContant.TOPIC)));
        Objects.requireNonNull(redisTemplate.getConnectionFactory()).getConnection().subscribe(new RedisPushListener(this), unwrap(Collections.singletonList(holder)));
    }

    protected byte[][] unwrap(Collection<ByteArrayWrapper> holders) {
        if (CollectionUtils.isEmpty(holders)) {
            return new byte[0][];
        }

        byte[][] unwrapped = new byte[holders.size()][];

        int index = 0;
        for (ByteArrayWrapper arrayHolder : holders) {
            unwrapped[index++] = arrayHolder.getArray();
        }
        return unwrapped;
    }
}
