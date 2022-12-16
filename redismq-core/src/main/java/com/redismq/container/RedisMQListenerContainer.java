package com.redismq.container;

import com.redismq.CompositeQueue;
import com.redismq.Message;
import com.redismq.core.RedisListenerRunnable;
import com.redismq.constant.AckMode;
import com.redismq.delay.DelayTimeoutTask;
import com.redismq.delay.DelayTimeoutTaskManager;
import com.redismq.exception.RedisMqException;
import com.redismq.factory.DefaultRedisListenerContainerFactory;
import com.redismq.queue.Queue;
import com.redismq.queue.QueueManager;
import com.redismq.utils.RedisMQObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.redismq.config.GlobalConfigCache.GLOBAL_CONFIG;
import static com.redismq.constant.GlobalConstant.*;
import static com.redismq.constant.RedisMQConstant.getVirtualQueueLock;
import static com.redismq.queue.QueueManager.INVOKE_VIRTUAL_QUEUES;

/**
 * @author hzh
 * @date 2021/8/10
 * redis延时队列实现   通过发布订阅和时间轮实现高性能。  一个queue对应一个端点对应多个queue:tag
 */
public class RedisMQListenerContainer extends AbstractMessageListenerContainer {
    protected static final Logger log = LoggerFactory.getLogger(RedisMQListenerContainer.class);
    /**
     * 延长锁看门狗
     */
    private final ScheduledThreadPoolExecutor lifeExtensionThread = new ScheduledThreadPoolExecutor(1);
    /**
     * 延时任务管理器
     */
    private final DelayTimeoutTaskManager delayTimeoutTaskManager = new DelayTimeoutTaskManager();
    private volatile ScheduledFuture<?> scheduledFuture;
    private final ThreadPoolExecutor work;

    /**
     * 停止
     */
    @Override
    public void doStop() {
        work.shutdown();
        delayTimeoutTaskManager.stop();
        try {
            if (!work.awaitTermination(WORK_THREAD_STOP_WAIT, TimeUnit.SECONDS)) {
                log.warn("redismq workThreadPool shutdown timeout");
                work.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("redismq workThreadPool shutdown error", e);
            Thread.currentThread().interrupt();
        }
    }

    public RedisMQListenerContainer(DefaultRedisListenerContainerFactory redisListenerContainerFactory, Queue registerQueue) {
        super(redisListenerContainerFactory, registerQueue);
        lifeExtension();
        work = new ThreadPoolExecutor(getConcurrency(), getMaxConcurrency(),
                60L, TimeUnit.SECONDS,
                // 这个范围内的视为核心线程可以处理
                new CompositeQueue<>(getConcurrency() << 3), new ThreadFactory() {
            private final ThreadGroup group;
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            private final String NAME_PREFIX = "REDISMQ-WORK-" + registerQueue.getQueueName() + "-";

            {
                SecurityManager s = System.getSecurityManager();
                group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            }

            @Override
            public Thread newThread(Runnable r) {
                //除了固定的boss线程。临时新增的线程会删除了会递增，int递增有最大值。这里再9999的时候就从固定线程的数量上重新计算.防止线程名字过长
                int current = threadNumber.getAndUpdate(operand -> operand >= THREAD_NUM_MAX ? getConcurrency() + 1 : operand + 1);
                Thread t = new Thread(group, r, NAME_PREFIX + current);
                t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        }, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 拉取队列消息
     *
     * @param queueName 队列名称
     * @return {@link Set}<{@link Long}>
     */
    public Set<Long> pop(String queueName) {
        Set<Long> startTimeSet = new HashSet<>();
        int count = 0;
        while (isRunning()) {
            try {
                //获取已经到时间要执行的任务  本地消息的数量相当于本地偏移量   localMessages.size()是指从这个位置之后开始啦
                long pullTime = System.currentTimeMillis();
                int size = localMessages.size();
                /*
                 此处会有部分本地消息正好被消费完,那么本地偏移量会前移几十个.那么会拉取到正在消费的消息
                解决思路1 记录偏移量的count 一直累加.直到取不到消息的时候偏移量归零重新取数据.
               解决思路2 本地消息必须全部提交,也就是全部被删除后,才能消费下一组消息.
               目前采用方案1，2整合
                 */
                Set<ZSetOperations.TypedTuple<Object>> tuples = redisClient.zRangeByScoreWithScores(queueName, 0, pullTime, count, super.maxConcurrency);
                if (CollectionUtils.isEmpty(tuples)) {
                    //响应中断
                    if (!isRunning()) {
                        return startTimeSet;
                    }
                    //本地消息没有消费完就先不取延时任务的.
                    if (size > 0) {
                        Thread.sleep(500L);
                        continue;
                    }
                    //从头的偏移量开始消费
                    if (count > 0) {
                        count = 0;
                        continue;
                    }

                    if (delay) {
                        //如果没有数据获取头部数据100条的时间.加入时间轮.到点的时候再过来取真实数据
                        Set<ZSetOperations.TypedTuple<Object>> headDatas = redisClient.zRangeWithScores(queueName, 0, GLOBAL_CONFIG.delayQueuePullSize);
                        if (headDatas != null) {
                            for (ZSetOperations.TypedTuple<Object> headData : headDatas) {
                                Double score = headData.getScore();
                                if (score != null) {
                                    startTimeSet.add(score.longValue());
                                }
                            }
                        }
                    }
                    break;
                }


                count += tuples.size();
                for (ZSetOperations.TypedTuple<Object> tuple : tuples) {
                    if (!isRunning()) {
                        break;
                    }
                    Message message = (Message) tuple.getValue();
                    if (message == null) {
                        continue;
                    }
                    //手动ack
                    try {
                        if (AckMode.MAUAL.equals(ackMode)) {
                            Message msg = localMessages.putIfAbsent(message.getId(), message);
                            if (msg != null) {
                                continue;
                            }
                        } else {
                            Long remove = redisClient.zRemove(queueName, message);
                            if (remove == null || remove <= 0) {
                                continue;
                            }
                        }
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            if (isRunning()) {
                                log.info("redisMQ acquire semaphore InterruptedException", e);
                            }
                            Thread.currentThread().interrupt();
                        }
                        String id = super.getRunableKey(message.getTag());
                        RedisListenerRunnable runnable = super.getRedisListenerRunnable(id, message);
                        if (runnable == null) {
                            throw new RedisMqException("redisMQ not found tag runnable");
                        }
                        // 多线程执行完毕后semaphore.release();
                        work.execute(runnable);
                    } catch (Exception e) {
                        if (isRunning()) {

                            log.error("redisMQ listener container error ", e);

                            //如果异常直接释放资源，否则线程执行完毕才释放
                            localMessages.remove(message.getId());

                            // 如果是框架中的异常,说明异常是不可修复的.删除异常的消息
                            if (e instanceof RedisMqException) {
                                log.error("RedisMqException removeMessage:{}", RedisMQObjectMapper.toJsonStr(message), e);
                                Long remove = redisClient.zRemove(queueName, message);
                            }
                        }
                        semaphore.release();
                    }
                }
            } catch (Exception e) {
                if (isRunning()) {
                    //报错需要  semaphore.release();
                    log.error("redisMQ pop error", e);
                    if (e.getMessage().contains("WRONGTYPE Operation against a key holding the wrong kind of value")) {
                        log.error("redisMQ [ERROR] queue not is zset type。 cancel pop");
                        stop();
                    }
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        return startTimeSet;
    }


    @Override
    public void repush() {
        throw new RedisMqException("延时队列不存在的方法  repush()");
    }


    /**
     * 加入开始任务
     *
     * @param virtualQueue 虚拟队列
     * @param startTime    开始时间
     */
    public void start(String virtualQueue, Long startTime) {

        running();

        // 虚拟队列锁定执行任务默认时长,有看门狗机制
        String virtualQueueLock = getVirtualQueueLock(virtualQueue);

        //这里被锁住,如果有服务下线了.需要超过这个时间消息才能继续被消费.因为会被锁定.
        Boolean success = redisClient.setIfAbsent(virtualQueueLock, "", Duration.ofSeconds(GLOBAL_CONFIG.virtualLockTime));
        if (GLOBAL_CONFIG.printConsumeLog) {
            log.info("current virtualQueue:{} lockSuccess:{} state:{}", virtualQueue, success, state);
        }

        //获取锁失败
        if (success == null || !success) {
            return;
        }

        //为空说明当前能获取到数据
        DelayTimeoutTask timeoutTask = delayTimeoutTaskManager.computeIfAbsent(virtualQueue, task -> new DelayTimeoutTask() {
            @Override
            protected Set<Long> pullTask() {
                try {
                    // 执行真正任务 加锁执行.超过200ms获取不到则放弃.加长这里的时长可以避免执行中的任务刚好拿完老的redis消息.而没有取到最新的消息.
                    // 而新的消息通知因为加锁获取不到的问题
                    // 加锁主要是为了避免多个订阅的消息同时进来要求拉取同一个队列的消息.改造为分布式锁.可以同时解决手动ack多个服务负载均衡错误的并发消费问题
                    List<String> virtualQueues = QueueManager.CURRENT_VIRTUAL_QUEUES.get(queueName);
                    if (CollectionUtils.isEmpty(virtualQueues)) {
                        return null;
                    }
                    if (!virtualQueues.contains(virtualQueue)) {
                        return null;
                    }

                    //添加到当前执行队列。看门狗用
                    INVOKE_VIRTUAL_QUEUES.add(virtualQueue);
                    Set<Long> pop = pop(virtualQueue);
                    //为空说明当前能获取到数据
                    return new HashSet<>(pop);
                } finally {
                    INVOKE_VIRTUAL_QUEUES.remove(virtualQueue);
                    redisClient.delete(virtualQueueLock);
                }
            }
        });
        try {
            delayTimeoutTaskManager.schedule(timeoutTask, startTime);
        } catch (Exception e) {
            log.error("delayTimeoutTaskManager schedule ", e);
            redisClient.delete(virtualQueueLock);
        }
    }

    /**
     * 消费锁续期 看门狗
     */
    private void lifeExtension() {
//        if (AckMode.MAUAL.equals(ackMode)) {
        if (scheduledFuture == null || scheduledFuture.isCancelled()) {
            scheduledFuture = lifeExtensionThread.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    List<String> virtualQueues = QueueManager.CURRENT_VIRTUAL_QUEUES.get(queueName);
                    if (CollectionUtils.isEmpty(virtualQueues)) {
                        return;
                    }
                    for (String virtualQueue : INVOKE_VIRTUAL_QUEUES) {
                        String lua = "if (redis.call('exists', KEYS[1]) == 1) then " +
                                "redis.call('expire', KEYS[1]," + GLOBAL_CONFIG.virtualLockTime + "); " +
                                "return 1; " +
                                "end; " +
                                "return 0;";
                        try {
                            List<String> list = new ArrayList<>();
                            list.add(getVirtualQueueLock(virtualQueue));
                            Long execute = redisClient.executeLua(lua, list);
                        } catch (Exception e) {
                            if (isRunning()) {
                                log.error("lifeExtension  redisTemplate.expire Exception", e);
                            }
                        }
                    }
                }
            }, GLOBAL_CONFIG.virtualLockWatchDogTime, GLOBAL_CONFIG.virtualLockWatchDogTime, TimeUnit.SECONDS);
        }
//        }
    }
}

