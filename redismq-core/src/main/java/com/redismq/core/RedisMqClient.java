package com.redismq.core;


import com.redismq.constant.RedisMQConstant;
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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RedisMqClient {
    private final ScheduledThreadPoolExecutor registerThread = new ScheduledThreadPoolExecutor(1);
    private final ScheduledThreadPoolExecutor rebalanceThread = new ScheduledThreadPoolExecutor(1);
    protected static final Logger log = LoggerFactory.getLogger(RedisMqClient.class);
    private final RedisListenerContainerManager redisListenerContainerManager;
    private static final String CLIENT_KEY = "REDISMQ_CLIENT";
    private final RedisTemplate<String, Object> redisTemplate;
    private final String clientId;
    private final RebalanceImpl rebalance;

    public RedisMqClient(RedisTemplate<String, Object> redisTemplate, RedisListenerContainerManager redisListenerContainerManager, RebalanceImpl rebalance) {
        this.redisTemplate = redisTemplate;
        this.clientId = ClientConfig.getLocalAddress();
        this.redisListenerContainerManager = redisListenerContainerManager;
        this.rebalance = rebalance;
    }

    public String getClientId() {
        return clientId;
    }

    public RedisListenerContainerManager getRedisListenerContainerManager() {
        return redisListenerContainerManager;
    }


    public void registerClient() {
        log.debug("registerClient :{}", clientId);
        Boolean success = redisTemplate.opsForZSet().addIfAbsent(CLIENT_KEY, clientId, System.currentTimeMillis());
        if (success == null || !success) {
            //注册客户端
            redisTemplate.opsForZSet().add(CLIENT_KEY, clientId, System.currentTimeMillis());
        } else {
            doRebalance();
        }
    }

    public Set<String> allClient() {
        return redisTemplate.opsForZSet().rangeByScore(CLIENT_KEY, 1, Double.MAX_VALUE).stream().map(Object::toString).collect(Collectors.toSet());
    }

    public Long removeExpireClients() {
        // 60秒以外的客户端
        long max = System.currentTimeMillis() - 40000L;
        return redisTemplate.opsForZSet().removeRangeByScore(CLIENT_KEY, 0, max);
    }

    public Long removeAllClient() {
        return redisTemplate.opsForZSet().removeRangeByScore(CLIENT_KEY, 0, Double.MAX_VALUE);
    }

    public void destory() {
        redisTemplate.opsForZSet().remove(CLIENT_KEY, clientId);
        log.info("redismq client remove");
        //停止任务
        redisListenerContainerManager.stopAll();
    }

    public void start() {
        // 清理所有客户端
        removeAllClient();
        // 订阅平衡消息
        rebalanceSubscribe();
        // 当前客户端暂时监听所有队列  等待下次重平衡所有队列.防止新加入客户端时.正好有客户端退出.而出现有几个队列在1分钟内没有客户端监听的情况
        registerClient();
        // 发布重平衡 会让其他服务暂停拉取消息
        publishRebalance();
        // 在执行重平衡.当前服务暂停重新分配拉取消息
//        doRebalance();
        // 30秒自动注册
        startRegisterClientTask();
        // 20秒自动重平衡
        startRebalanceTask();
        //订阅push消息
        subscribe();
        //启动队列监控
        redisListenerContainerManager.startRedisListener();
        //启动延时队列监控
        redisListenerContainerManager.startDelayRedisListener();
    }

    // 多个服务应该只有一个执行重平衡
    public void rebalance() {
        Long count = removeExpireClients();
        if (count != null && count > 0) {
            log.info("doRebalance removeExpireClients count=:{}", count);
            publishRebalance();
            doRebalance();
        }
    }

    // 暂停消息分配.重新负载均衡后.重新拉取消息
    public void doRebalance() {
        redisListenerContainerManager.pauseAll();
        rebalance.rebalance(allClient(), clientId);
        repush();
    }

    private void publishRebalance() {
        redisTemplate.convertAndSend(RedisMQConstant.REBALANCE_TOPIC, clientId);
    }

    //启动时对任务重新进行拉取
    public void repush() {
        Map<String, List<String>> queues = QueueManager.CURRENT_VIRTUAL_QUEUES;
        if (CollectionUtils.isEmpty(queues)) {
            return;
        }
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
        ByteArrayWrapper holder = new ByteArrayWrapper(Objects.requireNonNull(stringSerializer.serialize(RedisMQConstant.TOPIC)));
        Objects.requireNonNull(redisTemplate.getConnectionFactory()).getConnection().subscribe(new RedisPushListener(this), unwrap(Collections.singletonList(holder)));
    }

    public void rebalanceSubscribe() {
        RedisSerializer<String> stringSerializer = redisTemplate.getStringSerializer();
        ByteArrayWrapper byteArrayWrapper = new ByteArrayWrapper(Objects.requireNonNull(stringSerializer.serialize(RedisMQConstant.REBALANCE_TOPIC)));
        Objects.requireNonNull(redisTemplate.getConnectionFactory()).getConnection().subscribe(new RedisRebalanceListener(this), unwrap(Collections.singletonList(byteArrayWrapper)));
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

    public void startRegisterClientTask() {
        registerThread.scheduleAtFixedRate(this::registerClient, 30, 30, TimeUnit.SECONDS);
    }

    public void startRebalanceTask() {
        String lockKey = "RedisMQRebalanceLock";
        Boolean success = redisTemplate.opsForValue().setIfAbsent(lockKey, "", 20, TimeUnit.SECONDS);
        if (success != null && success) {
            registerThread.scheduleAtFixedRate(this::rebalance, 10, 20, TimeUnit.SECONDS);
        }
    }
}
