package com.redismq.core;


import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.redismq.connection.RedisClient;
import com.redismq.constant.PushMessage;
import com.redismq.constant.RedisMQConstant;
import com.redismq.queue.Queue;
import com.redismq.queue.QueueManager;
import com.redismq.rebalance.ClientConfig;
import com.redismq.rebalance.RebalanceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.redismq.config.GlobalConfigCache.GLOBAL_CONFIG;
import static com.redismq.constant.GlobalConstant.*;
import static com.redismq.constant.RedisMQConstant.*;

/**
 * @Author: hzh
 * @Date: 2022/11/4 16:44
 * RedisMQ客户端
 */
public class RedisMqClient {
    protected static final Logger log = LoggerFactory.getLogger(RedisMqClient.class);
    private final ScheduledThreadPoolExecutor registerThread = new ScheduledThreadPoolExecutor(1);
    private final ScheduledThreadPoolExecutor rebalanceThread = new ScheduledThreadPoolExecutor(1);
    private final RedisListenerContainerManager redisListenerContainerManager;
    private final RedisClient redisClient;
    private final String clientId;
    private final RebalanceImpl rebalance;
    private RedisMessageListenerContainer redisMessageListenerContainer;
    private boolean isSub;

    public RedisMqClient(RedisClient redisClient, RedisListenerContainerManager redisListenerContainerManager, RebalanceImpl rebalance) {
        this.redisClient = redisClient;
        this.clientId = ClientConfig.getLocalAddress() + SPLITE + NanoIdUtils.randomNanoId();
        this.redisListenerContainerManager = redisListenerContainerManager;
        this.rebalance = rebalance;
    }

    public void setRedisMessageListenerContainer(RedisMessageListenerContainer redisMessageListenerContainer) {
        this.redisMessageListenerContainer = redisMessageListenerContainer;
    }

    public String getClientId() {
        return clientId;
    }

    public RedisListenerContainerManager getRedisListenerContainerManager() {
        return redisListenerContainerManager;
    }


    public void registerClient() {
        log.debug("registerClient :{}", clientId);
        //注册客户端
        redisClient.zAdd(getClientKey(), clientId, System.currentTimeMillis());
    }

    public Set<String> allClient() {
        return redisClient.zRangeByScore(getClientKey(), 1, Double.MAX_VALUE).stream().map(Object::toString).collect(Collectors.toSet());
    }

    public Long removeExpireClients() {
        // 过期的客户端
        long max = System.currentTimeMillis() - CLIENT_EXPIRE * 1000L;
        return redisClient.zRemoveRangeByScore(getClientKey(), 0, max);
    }

    public Long removeAllClient() {
        log.info("redismq removeAllClient");
        return redisClient.zRemoveRangeByScore(getClientKey(), 0, Double.MAX_VALUE);
    }

    public void destory() {
        //停止任务
        redisClient.zRemove(getClientKey(), clientId);
        redisListenerContainerManager.stopAll();
        publishRebalance();
        log.info("redismq client remove currentVirtualQueues:{} ", QueueManager.getCurrentVirtualQueues());
    }

    public void start() {
        // 清理所有客户端
        removeAllClient();
        // 当前客户端暂时监听所有队列  等待下次重平衡所有队列.防止新加入客户端时.正好有客户端退出.而出现有几个队列在1分钟内没有客户端监听的情况 doReblance已经注册
//        registerClient();
        // 先订阅平衡消息,以免平衡的消息没有收到
        rebalanceSubscribe();
        // 重平衡
        rebalance();
        //订阅push消息
//        subscribe();
        // 30秒自动注册
        startRegisterClientTask();
        // 20秒自动重平衡
        startRebalanceTask();
        //启动队列监控
        redisListenerContainerManager.startRedisListener();
        //启动延时队列监控
        redisListenerContainerManager.startDelayRedisListener();
    }


    // 多个服务应该只有一个执行重平衡
    public void rebalanceTask() {
        String lockKey = getRebalanceLock();
        Boolean success = redisClient.setIfAbsent(lockKey, "", Duration.ofSeconds(CLIENT_RABALANCE_TIME));
        if (success != null && success) {
            Long count = removeExpireClients();
            if (count != null && count > 0) {
                log.info("doRebalance removeExpireClients count=:{}", count);
                rebalance();
                // 消费锁是30秒 这个值和消费所相关联
                // 延时指定消费锁锁定的时间再去重新拉取一次消息,防止服务下线重启导致的消息没有被其他队列消费的问题
                new ScheduledThreadPoolExecutor(1).schedule(this::repush, GLOBAL_CONFIG.virtualLockTime, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * 平衡
     */
    public void rebalance() {
        // 发布重平衡 会让其他服务暂停拉取消息
        publishRebalance();
        // 在执行重平衡.当前服务暂停重新分配拉取消息 放到注册客户端中
        doRebalance();
    }


    /**
     * 暂停消息分配.重新负载均衡后.重新拉取消息
     */
    public void doRebalance() {
        registerClient();
        redisListenerContainerManager.pauseAll();
        //临时解决重平衡问题.这里主要是因为有可能出现某些客户端还没注册进来 ，等200毫秒等他们都注册进来。不是好方法但是行得通。主要是这个时间不好确定。
        // 如果redis有延迟那么重平衡就有问题，那么后果就是消息分配不平均
        try {
            Thread.sleep(200L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        rebalance.rebalance(allClient(), clientId);
        repush();
    }

    private void publishRebalance() {
        redisClient.convertAndSend(getRebalanceTopic(), clientId);
    }


    /**
     * 重平衡时对任务重新进行拉取
     */
    public void repush() {
        Map<String, List<String>> queues = QueueManager.getCurrentVirtualQueues();
        boolean isEmpty = queues.values().stream().allMatch(CollectionUtils::isEmpty);

        //没有监听的队列取消订阅
        if (isEmpty) {
            unSubscribe();
            return;
        }

        //监听队列消息的订阅
        subscribe();

        //此操作 On2 如果有几千个虚拟队列的话。那么最少也要有几百个topic 这里性能不会慢。但是另一边监听到会去redis中获取。线程数不多可能会阻塞
        queues.forEach((k, v) -> {
            Queue queue = QueueManager.getQueue(k);
            if (queue == null) {
                log.error("repush queue is null");
                return;
            }
            List<String> virtualQueues = QueueManager.getCurrentVirtualQueues().get(k);
            if (CollectionUtils.isEmpty(virtualQueues)) {
                return;
            }
            //获取虚拟队列重新推送到阻塞队列
            virtualQueues.forEach(vq -> {
                        PushMessage pushMessage = new PushMessage();
                        pushMessage.setQueue(vq);
                        pushMessage.setTimestamp(System.currentTimeMillis());

                        //推送到指定的队列
                        LinkedBlockingQueue<PushMessage> delayBlockingQueue = redisListenerContainerManager.getDelayBlockingQueue();
                        LinkedBlockingQueue<String> linkedBlockingQueue = redisListenerContainerManager.getLinkedBlockingQueue();
                        if (queue.isDelayState()) {
                            delayBlockingQueue.add(pushMessage);
                        } else {
                            linkedBlockingQueue.add(vq);
                        }
                    }
            );
        });
    }

    /**
     * 监听队列消息的订阅
     */
    public synchronized void subscribe() {
        if (!isSub) {
            RedisMqClient redisMqClient = this;
            redisMessageListenerContainer.addMessageListener(new RedisPushListener(redisMqClient), new ChannelTopic(RedisMQConstant.getTopic()));
            isSub = true;
        }
    }

    /**
     * 取消监听队列消息的订阅
     */
    public synchronized void unSubscribe() {
        if (isSub) {
            RedisMqClient redisMqClient = this;
            redisMessageListenerContainer.removeMessageListener(new RedisPushListener(redisMqClient), new ChannelTopic(RedisMQConstant.getTopic()));
            isSub = false;
        }
    }

    /**
     * 负载均衡订阅
     */
    public void rebalanceSubscribe() {
        RedisMqClient redisMqClient = this;
        redisMessageListenerContainer.addMessageListener(new RedisRebalanceListener(redisMqClient), new ChannelTopic(RedisMQConstant.getRebalanceTopic()));
    }

    /**
     * 开始注册客户任务   心跳任务
     */
    public void startRegisterClientTask() {
        registerThread.scheduleAtFixedRate(this::registerClient, CLIENT_REGISTER_TIME, CLIENT_REGISTER_TIME, TimeUnit.SECONDS);
    }

    /**
     * 开始负载均衡任务
     */
    public void startRebalanceTask() {
        rebalanceThread.scheduleAtFixedRate(this::rebalanceTask, CLIENT_RABALANCE_TIME, CLIENT_RABALANCE_TIME, TimeUnit.SECONDS);
    }

    /**
     * 队列寄存器
     *
     * @param queue 队列
     */
    public Queue registerQueue(Queue queue) {
        Set<Queue> allQueue = getAllQueue();
        allQueue.stream().filter(redisQueue -> redisQueue.getQueueName().equals(queue.getQueueName())).forEach(redisQueue ->
                redisClient.sRemove(getQueueTopicKey(), redisQueue));
        redisClient.sAdd(getQueueTopicKey(), queue);
        return queue;
    }

    public Set<Queue> getAllQueue() {
        Set<Object> set = redisClient.sMembers(getQueueTopicKey());
        if (CollectionUtils.isEmpty(set)) {
            return new HashSet<>();
        }
        return set.stream().map(s -> (Queue) s).collect(Collectors.toSet());
    }
}
