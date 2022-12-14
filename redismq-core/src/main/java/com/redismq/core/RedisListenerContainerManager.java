package com.redismq.core;

import com.redismq.constant.PushMessage;
import com.redismq.container.AbstractMessageListenerContainer;
import com.redismq.container.RedisMQListenerContainer;
import com.redismq.exception.RedisMqException;
import com.redismq.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.redismq.config.GlobalConfigCache.GLOBAL_CONFIG;
import static com.redismq.constant.GlobalConstant.*;
import static com.redismq.constant.RedisMQConstant.getVirtualQueueLock;
import static com.redismq.constant.StateConstant.RUNNING;
import static com.redismq.queue.QueueManager.INVOKE_VIRTUAL_QUEUES;

/**
 * @author hzh
 * @date 2021/8/10
 * 监听器容器注册类。一个或多个boss线程轮询从redis队列中取数据。交给多个监听器多个线程池进行消费
 */
public class RedisListenerContainerManager {
    protected static final Logger log = LoggerFactory.getLogger(RedisListenerEndpoint.class);
    //延时队列每次的携带队列和时间戳.数据可能会不同.队列数量要求比较高
    private final LinkedBlockingQueue<PushMessage> delayBlockingQueue = new LinkedBlockingQueue<>(DELAY_BLOCKING_QUEUE_SIZE);
    private final LinkedBlockingQueue<String> linkedBlockingQueue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_SIZE);
    private ThreadPoolExecutor boss;
    // 队列的容器
    private final Map<String, AbstractMessageListenerContainer> redisDelayListenerContainerMap = new ConcurrentHashMap<>();
    private volatile int state = 0;

    public LinkedBlockingQueue<String> getLinkedBlockingQueue() {
        return linkedBlockingQueue;
    }

    public LinkedBlockingQueue<PushMessage> getDelayBlockingQueue() {
        return delayBlockingQueue;
    }

    public RedisMQListenerContainer getRedisPublishDelayListenerContainer(String queueName) {
        return (RedisMQListenerContainer) redisDelayListenerContainerMap.get(queueName);
    }

    public RedisListenerContainerManager() {
        setBoss(BOSS_NUM);
    }

    /*
              boss线程数
           */
    public void setBoss(int bossNum) {
        if (bossNum > BOSS_NUM) {
            throw new RedisMqException("boos线程初始化超过最大限制");
        }
        boss = new ThreadPoolExecutor(bossNum, BOSS_NUM,
                10L, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new ThreadFactory() {

            private final ThreadGroup group;
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            private static final String NAME_PREFIX = "REDISMQ-BOSS-";

            {
                SecurityManager s = System.getSecurityManager();
                group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            }

            @Override
            public Thread newThread(Runnable r) {
                //除了固定的boss线程。临时新增的线程会删除了会递增，int递增有最大值。这里再9999的时候就从固定线程的数量上重新计算
                int current = threadNumber.getAndUpdate(operand -> operand >= THREAD_NUM_MAX ? bossNum + 1 : operand + 1);
                Thread t = new Thread(group, r, NAME_PREFIX + current);
                t.setDaemon(true);
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        }, new ThreadPoolExecutor.DiscardPolicy());
    }

    boolean isActive() {
        return state == RUNNING;
    }

    void running() {
        state = RUNNING;
    }

    /**
     * 开始轮询监听队列消息
     */
    public void startRedisListener() {
        boss.execute(() -> {
            running();
            while (isActive()) {
                try {
                    String virtualName = linkedBlockingQueue.take();
                    RedisMQListenerContainer container = getRedisPublishDelayListenerContainer(QueueManager.getQueueNameByVirtual(virtualName));
                    boolean contains = isContains(virtualName);
                    int count = 0;
                    // 如果消费者暂停了，重试几次。以防止暂停消息的延时导致的停止消费。这个方案可能不完善。停止消费但INVOKE_VIRTUAL_QUEUES还没被删除
                    while (contains && container.isPause()) {
                        if (count > 3) {
                            break;
                        }
                        Thread.sleep(1000L);
                        contains = INVOKE_VIRTUAL_QUEUES.contains(virtualName);
                        if (GLOBAL_CONFIG.printConsumeLog) {
                            log.info("invoke_virtual_queues exclusive virtualName:{} count:{} retryContains:{}", virtualName, count, contains);
                        }
                        count++;
                    }
                    if (!contains) {
                        container.start(virtualName, System.currentTimeMillis());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private boolean isContains(String virtualName) {
        return INVOKE_VIRTUAL_QUEUES.contains(virtualName);
    }


    public void startDelayRedisListener() {
        boss.execute(() -> {
            running();
            while (isActive()) {
                try {
                    PushMessage pushMessage = delayBlockingQueue.take();
                    RedisMQListenerContainer container = getRedisPublishDelayListenerContainer(QueueManager.getQueueNameByVirtual(pushMessage.getQueue()));
                    container.start(pushMessage.getQueue(), pushMessage.getTimestamp());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    public void registerContainer(AbstractMessageListenerContainer listenerContainer, List<RedisListenerEndpoint> redisListenerEndpoints) {
        //id是队列的名称
        AbstractMessageListenerContainer container = redisDelayListenerContainerMap.computeIfAbsent(listenerContainer.getQueueName(), c -> listenerContainer);
        // 端点的容器 id是queue:tag
        Map<String, RedisListenerEndpoint> redisListenerEndpointMap = redisListenerEndpoints.stream().collect(
                Collectors.toMap(RedisListenerEndpoint::getId, r -> r));
        container.setRedisListenerEndpointMap(redisListenerEndpointMap);
    }

    public void stopAll() {
        redisDelayListenerContainerMap.values().forEach(r -> {
            List<String> list = new ArrayList<>();
            for (String virtualQueue : INVOKE_VIRTUAL_QUEUES) {
                list.add(getVirtualQueueLock(virtualQueue));
            }
            r.stop();
            r.redisClient().delete(list);
        });
        boss.shutdownNow();
        log.info("Shutdown  redismq All ThreadPollExecutor");
    }

    public void pauseAll() {
        redisDelayListenerContainerMap.values().forEach(r -> {


            //改为重试来获取消息
//            INVOKE_VIRTUAL_QUEUES.clear();
            r.pause();
            log.info("pause queue:{}", r.getQueueName());

            // 删除队列锁
            List<String> list = new ArrayList<>();
            INVOKE_VIRTUAL_QUEUES.forEach(virtualQueue -> list.add(getVirtualQueueLock(virtualQueue)));
            r.redisClient().delete(list);
        });
    }
}
