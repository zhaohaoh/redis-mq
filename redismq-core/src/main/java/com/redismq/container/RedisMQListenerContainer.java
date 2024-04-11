package com.redismq.container;

import com.redismq.CompositeQueue;
import com.redismq.Message;
import com.redismq.connection.RedisMQClientUtil;
import com.redismq.constant.AckMode;
import com.redismq.core.RedisListenerCallable;
import com.redismq.delay.DelayTimeoutTask;
import com.redismq.delay.DelayTimeoutTaskManager;
import com.redismq.exception.RedisMqException;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.queue.Queue;
import com.redismq.queue.QueueManager;
import com.redismq.utils.RedisMQStringMapper;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.redismq.config.GlobalConfigCache.GLOBAL_CONFIG;
import static com.redismq.constant.GlobalConstant.THREAD_NUM_MAX;
import static com.redismq.constant.GlobalConstant.WORK_THREAD_STOP_WAIT;
import static com.redismq.constant.RedisMQConstant.getVirtualQueueLock;
import static com.redismq.queue.QueueManager.INVOKE_VIRTUAL_QUEUES;


/**
 * @author hzh
 * @date 2021/8/10 redis延时队列实现   通过发布订阅和时间轮实现高性能。  一个queue对应一个端点对应多个queue:tag
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
    
    public RedisMQListenerContainer(RedisMQClientUtil redisMQClientUtil, Queue queue,
            List<ConsumeInterceptor> consumeInterceptorList) {
        super(redisMQClientUtil, queue, consumeInterceptorList);
        lifeExtension();
        work = new ThreadPoolExecutor(getConcurrency(), getMaxConcurrency(), 60L, TimeUnit.SECONDS,
                // 这个范围内的视为核心线程可以处理 队列的数量
                new CompositeQueue<>(getConcurrency() << 3), new ThreadFactory() {
            private final ThreadGroup group;
            
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            
            private final String NAME_PREFIX = "REDISMQ-WORK-" + queue.getQueueName() + "-";
            
            {
                SecurityManager s = System.getSecurityManager();
                group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            }
            
            @Override
            public Thread newThread(Runnable r) {
                //除了固定的boss线程。临时新增的线程会删除了会递增，int递增有最大值。这里再9999的时候就从固定线程的数量上重新计算.防止线程名字过长
                int current = threadNumber
                        .getAndUpdate(operand -> operand >= THREAD_NUM_MAX ? getConcurrency() + 1 : operand + 1);
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
    public Set<Long> pull(String queueName) {
        Set<Long> delayTimes = new LinkedHashSet<>();
        List<Future<Boolean>> futures = new ArrayList<>();
        while (isRunning()) {
            try {
                //获取已经到时间要执行的任务  本地消息的数量相当于本地偏移量   localMessages.size()是指从这个位置之后开始啦
                long pullTime = System.currentTimeMillis();
                
                futures.removeIf(Future::isDone);
                
                int pullSize = super.maxConcurrency - futures.size();
                
                //说明消费队列已经满了 等待所有任务消费完成，然后再继续拉取消息.后面优化加个最长等待时间。是针对每个任务的。可以动态控制。如果超时的话任务就取消丢弃。
                if (pullSize <= 0) {
                    pullSize = waitConsume(futures, GLOBAL_CONFIG.getTaskTimeout(), true);
                }
   
                List<Message> messages = redisMQClientUtil.pullMessage(queueName, pullTime, futures.size(), pullSize);
                
                if (CollectionUtils.isEmpty(messages)) {
                    //响应中断
                    if (!isRunning()) {
                        return delayTimes;
                    }
                    //如果消费未完成 等待消费完成，然后再继续拉取消息.  只有手动消费模式才需要
                    if (futures.size() > 0 && ackMode.equals(AckMode.MAUAL)) {
                        waitConsume(futures, GLOBAL_CONFIG.getTaskWaitTime(), false);
                        continue;
                    }
                    
                    if (delay) {
                        //如果没有数据获取头部数据100条的时间.加入时间轮.到点的时候再过来取真实数据
                        List<Pair<Message, Double>> pairs = redisMQClientUtil
                                .pullMessageByTimeWithScope(queueName, pullTime, 0, GLOBAL_CONFIG.delayQueuePullSize);
                        pairs.forEach((pair -> delayTimes.add(pair.getValue().longValue())));
                    }
                    break;
                }
                
                List<RedisListenerCallable> callableInvokes = new ArrayList<>();
                for (Message message : messages) {
                    if (!isRunning()) {
                        break;
                    }
                    if (message == null) {
                        continue;
                    }
                    
                    try {
                        //手动ack
                        if (AckMode.MAUAL.equals(ackMode)) {
                        
                        } else {
                            Long remove = redisMQClientUtil.removeMessage(queueName, message.getId());
                            if (remove == null || remove <= 0) {
                                continue;
                            }
                        }
                        String id = super.getRunableKey(message.getTag());
                        RedisListenerCallable callable = super.getRedisListenerCallable(id, message);
                        if (callable == null) {
                            // 如果是框架中的异常,说明异常是不可修复的.删除异常的消息
                            redisMQClientUtil.removeMessage(queueName, message.getId());
                            log.error("RedisMqException   not found queue or tag removeMessage:{}",
                                    RedisMQStringMapper.toJsonStr(message));
                            continue;
                        }
                        
                        callableInvokes.add(callable);
                    } catch (Exception e) {
                        if (isRunning()) {
                            log.error("redisMQ listener container error ", e);
                        }
                    }
                }
                if (CollectionUtils.isEmpty(callableInvokes)) {
                    if (isRunning()) {
                        log.error("redisMQ callableInvokes isEmpty queueName:{}", queueName);
                    }
                    continue;
                }
                
                for (RedisListenerCallable callableInvoke : callableInvokes) {
                    Future<Boolean> submit = work.submit(callableInvoke);
                    futures.add(submit);
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
        return delayTimes;
    }
    
    /**
     * 等待消费任务完成
     *
     * @return int
     */
    private int waitConsume(List<Future<Boolean>> futures, long milliseconds, boolean timeoutDrop) {
        int pullSize;
        futures.removeIf(f -> {
            try {
                return f.get(milliseconds, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                return timeoutDrop;
            }
        });
        pullSize = super.maxConcurrency;
        return pullSize;
    }
    
    
    @Override
    public void repush() {
        throw new RedisMqException("延时队列不存在的方法  repush()");
    }
    
    
    /**
     * 开始拉取消息的任务。同一个时间一个虚拟队列只允许一个服务的一个线程进行拉取操作
     *
     * @param virtualQueue 虚拟队列
     * @param startTime    开始时间
     */
    public void start(String virtualQueue, Long startTime) {
        
        running();
        
        // 虚拟队列锁定执行任务默认时长,有看门狗机制
        String virtualQueueLock = getVirtualQueueLock(virtualQueue);
        
        // 这里被锁住,如果有服务下线了.需要超过这个时间消息才能继续被消费.因为会被锁定.
        Boolean success = lockQueue(virtualQueueLock);
        if (GLOBAL_CONFIG.printConsumeLog) {
            log.info("current virtualQueue:{} tryLockSuccess:{} state:{}", virtualQueue, success, state);
        }
        //获取锁失败
        if (success == null || !success) {
            return;
        }
        
        //为空说明当前能获取到数据
        DelayTimeoutTask timeoutTask = delayTimeoutTaskManager
                .computeIfAbsent(virtualQueue, task -> new DelayTimeoutTask() {
                    @Override
                    protected Set<Long> pullTask() {
                        try {
                            List<String> virtualQueues = QueueManager.getCurrentVirtualQueues().get(queueName);
                            if (CollectionUtils.isEmpty(virtualQueues)) {
                                return null;
                            }
                            if (!virtualQueues.contains(virtualQueue)) {
                                return null;
                            }
                            
                            //添加到当前执行队列。看门狗用
                            INVOKE_VIRTUAL_QUEUES.add(virtualQueue);
                            Set<Long> delayTimes = pull(virtualQueue);
                            //为空说明当前能获取到数据
                            return new LinkedHashSet<>(delayTimes);
                        } finally {
                            INVOKE_VIRTUAL_QUEUES.remove(virtualQueue);
                            unLockQueue(virtualQueueLock);
                        }
                    }
                });
        try {
            delayTimeoutTaskManager.schedule(timeoutTask, startTime);
        } catch (Exception e) {
            log.error("delayTimeoutTaskManager schedule ", e);
            unLockQueue(virtualQueueLock);
        }
    }
    
    //获取锁和释放锁的动作只能有一个线程执行 后面要优化成redisson
    private void unLockQueue(String virtualQueueLock) {
        redisMQClientUtil.unlock(virtualQueueLock);
    }
    //获取锁和释放锁的动作只能有一个线程执行 后面要优化成redisson
    private Boolean lockQueue(String virtualQueueLock) {
        int i=0;
        Boolean lock = redisMQClientUtil.lock(virtualQueueLock, Duration.ofSeconds(GLOBAL_CONFIG.virtualLockTime));
        while (!lock && i<2){
            i++;
            try {
                Thread.sleep(200L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            redisMQClientUtil.lock(virtualQueueLock, Duration.ofSeconds(GLOBAL_CONFIG.virtualLockTime));
        }
        return lock;
    }
    
    /**
     * 消费锁续期 看门狗
     */
    private void lifeExtension() {
        if (scheduledFuture == null || scheduledFuture.isCancelled()) {
            scheduledFuture = lifeExtensionThread.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    List<String> virtualQueues = QueueManager.getCurrentVirtualQueues().get(queueName);
                    if (CollectionUtils.isEmpty(virtualQueues)) {
                        return;
                    }
                    for (String virtualQueue : INVOKE_VIRTUAL_QUEUES) {
                        String lua = "if (redis.call('exists', KEYS[1]) == 1) then " + "redis.call('expire', KEYS[1],"
                                + GLOBAL_CONFIG.virtualLockTime + "); " + "return 1; " + "end; " + "return 0;";
                        try {
                            List<String> list = new ArrayList<>();
                            list.add(getVirtualQueueLock(virtualQueue));
                            redisMQClientUtil.executeLua(lua, list);
                        } catch (Exception e) {
                            if (isRunning()) {
                                log.error("lifeExtension  redisTemplate.expire Exception", e);
                            }
                        }
                    }
                }
            }, GLOBAL_CONFIG.virtualLockWatchDogTime, GLOBAL_CONFIG.virtualLockWatchDogTime, TimeUnit.SECONDS);
        }
    }
}

