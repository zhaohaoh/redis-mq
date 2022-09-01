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
    private final LinkedBlockingQueue<PushMessage> delayBlockingQueue = new LinkedBlockingQueue<>(65536);
    private final LinkedBlockingQueue<String> linkedBlockingQueue = new LinkedBlockingQueue<>(2048);
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
        setBoss(2);
    }

    /*
              boss线程数
           */
    public void setBoss(int bossNum) {
        if (bossNum > 2) {
            throw new RedisMqException("boos线程初始化超过最大限制");
        }
        boss = new ThreadPoolExecutor(bossNum, 2,
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
                int current = threadNumber.getAndUpdate(operand -> operand >= 99999 ? bossNum + 1 : operand + 1);
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

    public void startRedisListener() {
        boss.execute(() -> {
            running();
            while (isActive()) {
                try {
                    String virtualName = linkedBlockingQueue.take();
                    RedisMQListenerContainer container = getRedisPublishDelayListenerContainer(QueueManager.getQueueNameByVirtual(virtualName));
                    boolean contains = INVOKE_VIRTUAL_QUEUES.contains(virtualName);
                    int count = 0;
                    while (contains && container.isPause()) {
                        if (count > 3) {
                            break;
                        }
                        Thread.sleep(1000L);
                        contains = INVOKE_VIRTUAL_QUEUES.contains(virtualName);
                        log.info("invoke_virtual_queues exclusive virtualName:{} count:{} retryContains:{}", virtualName, count, contains);
                        count++;
                    }
                    log.info("invoke_virtual_queues virtualName:{} count:{} isPause:{}  ", virtualName, count, container.isPause());
                    if (!contains) {
                        container.start(virtualName, System.currentTimeMillis());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
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
            r.getRedisTemplate().delete(list);
        });
        boss.shutdownNow();
        log.info("Shutdown  redismq All ThreadPollExecutor");
    }

    public void pauseAll() {
        redisDelayListenerContainerMap.values().forEach(r -> {
            List<String> list = new ArrayList<>();
            for (String virtualQueue : INVOKE_VIRTUAL_QUEUES) {
                list.add(getVirtualQueueLock(virtualQueue));
            }
            //改为重试来获取消息
//            INVOKE_VIRTUAL_QUEUES.clear();
            r.pause();
            log.info("暂停状态 queue:{}", r.getQueueName());
            r.getRedisTemplate().delete(list);
        });
    }
}
