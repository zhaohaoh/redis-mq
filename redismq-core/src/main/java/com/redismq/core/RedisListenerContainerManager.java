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
    protected static final Logger log = LoggerFactory.getLogger(RedisListenerContainerManager.class);
    /**
     * 延时队列每次的携带队列和时间戳.数据可能会不同.队列数量要求比较高
     */
    private final LinkedBlockingQueue<PushMessage> delayBlockingQueue = new LinkedBlockingQueue<>(DELAY_BLOCKING_QUEUE_SIZE);
    /**
     * 普通消息队列
     */
    private final LinkedBlockingQueue<String> linkedBlockingQueue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_SIZE);
    /**
     * boss线程 负责调度普通队列和线程队列。获取队列开启拉取任务的消息
     */
    private ThreadPoolExecutor boss;
    /**
     * 队列的容器
     */
    private final Map<String, AbstractMessageListenerContainer> redisListenerContainerMap = new ConcurrentHashMap<>();
    /**
     * 状态 目前没有
     */
    private volatile int state = 0;

    public LinkedBlockingQueue<String> getLinkedBlockingQueue() {
        return linkedBlockingQueue;
    }

    public LinkedBlockingQueue<PushMessage> getDelayBlockingQueue() {
        return delayBlockingQueue;
    }

    public RedisMQListenerContainer getRedisistenerContainer(String queueName) {
        return (RedisMQListenerContainer) redisListenerContainerMap.get(queueName);
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

    boolean isRunning() {
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
            while (isRunning()) {
                try {
                    String virtualName = linkedBlockingQueue.take();
                    RedisMQListenerContainer container = getRedisistenerContainer(QueueManager.getQueueNameByVirtual(virtualName));
                    container.start(virtualName, System.currentTimeMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error("RedisListener take queue error", e);
                }
            }
        });
    }


    /**
     * 开始轮询监听延时队列消息
     */
    public void startDelayRedisListener() {
        boss.execute(() -> {
            running();
            while (isRunning()) {
                try {
                    PushMessage pushMessage = delayBlockingQueue.take();
                    RedisMQListenerContainer container = getRedisistenerContainer(QueueManager.getQueueNameByVirtual(pushMessage.getQueue()));
                    container.start(pushMessage.getQueue(), pushMessage.getTimestamp());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error("DelayRedisListener take queue error", e);
                }
            }
        });
    }

    public void registerContainer(AbstractMessageListenerContainer listenerContainer, List<RedisListenerEndpoint> redisListenerEndpoints) {
        //id是队列的名称
        AbstractMessageListenerContainer container = redisListenerContainerMap.computeIfAbsent(listenerContainer.getQueueName(), c -> listenerContainer);
        // 端点的容器 id是queue:tag
        Map<String, RedisListenerEndpoint> redisListenerEndpointMap = redisListenerEndpoints.stream().collect(
                Collectors.toMap(RedisListenerEndpoint::getId, r -> r));
        container.setRedisListenerEndpointMap(redisListenerEndpointMap);
    }


    /**
     * 全部停止 RedisListenerContainer.start方法中会自动退出并且删除redis的key
     */
    public void stopAll() {
        redisListenerContainerMap.values().forEach(r -> {
            r.stop();
            List<String> list = new ArrayList<>();
            for (String virtualQueue : INVOKE_VIRTUAL_QUEUES) {
                list.add(getVirtualQueueLock(virtualQueue));
            }
            for (String lock : list) {
                r.getRedisMQClientUtil().unlock(lock);
            }
        });
        boss.shutdownNow();
        log.info("redisMQ shutdown All ThreadPollExecutor");
    }

    /**
     * 暂停所有  RedisListenerContainer.start方法中会自动退出并且删除redis的key
     * 需要注意这里删除了队列的锁。说明任何人可以获取到。那么就存在有任务正在线程中消费。从而存在重复消费
     */
    public void pauseAll() {
        redisListenerContainerMap.values().forEach(r -> {
            if (log.isDebugEnabled()) {
                log.debug("redisMQ pause queue:{}", r.getQueueName());
            }
            r.pause();
            // 删除队列锁
            List<String> list = new ArrayList<>();
            INVOKE_VIRTUAL_QUEUES.forEach(virtualQueue -> list.add(getVirtualQueueLock(virtualQueue)));
            for (String lock : list) {
                r.getRedisMQClientUtil().unlock(lock);
            }
        });
    }
}
