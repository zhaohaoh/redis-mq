package com.redismq.container;


import com.redismq.Message;
import com.redismq.core.RedisListenerRunnable;
import com.redismq.constant.AckMode;
import com.redismq.delay.DelayTimeoutTask;
import com.redismq.delay.DelayTimeoutTaskManager;
import com.redismq.exception.RedisMqException;
import com.redismq.factory.DefaultRedisListenerContainerFactory;
import com.redismq.queue.Queue;
import com.redismq.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.redismq.constant.RedisMQConstant.getVirtualQueueLock;
import static com.redismq.queue.QueueManager.INVOKE_VIRTUAL_QUEUES;

/**
 * @author hzh
 * @date 2021/8/10
 * redis延时队列实现   通过发布订阅和时间轮实现高性能。  一个queue对应一个端点对应多个queue:tag
 */
public class RedisMQListenerContainer extends AbstractMessageListenerContainer {
    protected static final Logger log = LoggerFactory.getLogger(RedisMQListenerContainer.class);
    private final ScheduledThreadPoolExecutor lifeExtensionThread = new ScheduledThreadPoolExecutor(1);
    private final DelayTimeoutTaskManager delayTimeoutTaskManager = new DelayTimeoutTaskManager();
    private volatile ScheduledFuture<?> scheduledFuture;
    private final ThreadPoolExecutor boss = new ThreadPoolExecutor(1, 1,
            60L, TimeUnit.SECONDS,
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
            Thread t = new Thread(group, r, NAME_PREFIX + threadNumber.getAndIncrement());
            t.setDaemon(true);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    });
    // 非核心线程的高并发驻留时间加长为10分钟
    private final ThreadPoolExecutor work;

    @Override
    public void doStop() {
        work.shutdown();
        try {
            if (!work.awaitTermination(3L, TimeUnit.SECONDS)) {
                log.warn("redismq workThreadPool shutdown timeout");
                work.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("redismq workThreadPool shutdown error", e);
            Thread.currentThread().interrupt();
        }
        delayTimeoutTaskManager.stop();
        //只要是线程休眠的状态就会停止
        boss.shutdownNow();
    }

    public RedisMQListenerContainer(DefaultRedisListenerContainerFactory redisListenerContainerFactory, Queue registerQueue) {
        super(redisListenerContainerFactory, registerQueue);
        lifeExtension();
        work = new ThreadPoolExecutor(getConcurrency(), getMaxConcurrency() + 1,
                600L, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new ThreadFactory() {
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
                int current = threadNumber.getAndUpdate(operand -> operand >= 99999 ? getConcurrency() + 1 : operand + 1);
                Thread t = new Thread(group, r, NAME_PREFIX + current);
                t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        }, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public Set<Long> pop(String queueName) {
        Set<Long> startTimeSet = new HashSet<>();
        int count = 0;
        while (isRunning()) {
            try {
                //获取已经到时间要执行的任务  本地消息的数量相当于本地偏移量   localMessages.size()是指从这个位置之后开始啦
                long pullTime = System.currentTimeMillis();
                int size = localMessages.size();
                // 此处会有部分本地消息正好被消费完,那么本地偏移量会前移几十个.那么会拉取到正在消费的消息
                // 解决思路1 记录偏移量的count 一直累加.直到取不到消息的时候偏移量归零重新取数据.
                //解决思路2 本地消息必须全部提交,也就是全部被删除后,才能消费下一组消息.
                Set<ZSetOperations.TypedTuple<Object>> tuples = redisTemplate.opsForZSet().rangeByScoreWithScores(queueName, 0, pullTime, count, super.maxConcurrency);
                if (CollectionUtils.isEmpty(tuples)) {
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
                        Set<ZSetOperations.TypedTuple<Object>> headDatas = redisTemplate.opsForZSet().rangeWithScores(queueName, 0, 100);
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
                    Message message = (Message) tuple.getValue();
                    //手动ack
                    try {
                        if (AckMode.MAUAL.equals(ackMode)) {
                            Message msg = localMessages.putIfAbsent(message.getId(), message);
                            if (msg != null) {
                                continue;
                            }
                        } else {
                            Long remove = redisTemplate.opsForZSet().remove(queueName, message);
                            if (remove == null || remove <= 0) {
                                continue;
                            }
                        }
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            if (isRunning()) {
                                log.info("redismq acquire semaphore InterruptedException", e);
                            }
                            Thread.currentThread().interrupt();
                        }
                        String id = super.getRunableKey(message.getTag());
                        RedisListenerRunnable runnable = super.getRedisListenerRunnable(id, message);
                        if (runnable == null) {
                            throw new RedisMqException("redismq not found tag runnable");
                        }
                        // 多线程执行完毕后semaphore.release();
                        work.execute(runnable);
                    } catch (Exception e) {
                        if (isRunning()) {
                            log.error("redismq listener container error ", e);
                            //如果异常直接释放资源，否则线程执行完毕才释放
                            localMessages.remove(message.getId());
                        }
                        semaphore.release();
                    }
                }
            } catch (Exception e) {
                if (isRunning()) {
                    //报错需要  semaphore.release();
                    log.error("redismq pop error", e);
                    if (e.getMessage().contains("WRONGTYPE Operation against a key holding the wrong kind of value")) {
                        log.error("redismq [ERROR] queue not is zset type。 cancel pop");
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


    public void start(String virtualQueue, Long startTime) {
        running();
        //为空说明当前能获取到数据
        DelayTimeoutTask task1 = delayTimeoutTaskManager.computeIfAbsent(virtualQueue, task -> new DelayTimeoutTask() {
            @Override
            protected Set<Long> pullTask() {
                // 执行真正任务 加锁执行.超过200ms获取不到则放弃.加长这里的时长可以避免执行中的任务刚好拿完老的redis消息.而没有取到最新的消息.
                // 而新的消息通知因为加锁获取不到的问题
                // 加锁主要是为了避免多个订阅的消息同时进来要求拉取同一个队列的消息.改造为分布式锁.可以同时解决手动ack多个服务负载均衡错误的并发消费问题
                String virtualQueueLock = getVirtualQueueLock(virtualQueue);
                Boolean success = redisTemplate.opsForValue().setIfAbsent(virtualQueueLock, "", Duration.ofSeconds(60));
                try {
                    if (success != null && success && isRunning()) {
                        List<String> virtualQueues = QueueManager.CURRENT_VIRTUAL_QUEUES.get(queueName);
                        if (CollectionUtils.isEmpty(virtualQueues)) {
                            return null;
                        }
                        if (!virtualQueues.contains(virtualQueue)) {
                            return null;
                        }
                        Set<Long> pop = pop(virtualQueue);
                        //为空说明当前能获取到数据
                        return new HashSet<>(pop);
                    }
                } finally {
                    if (success != null && success) {
                        INVOKE_VIRTUAL_QUEUES.remove(virtualQueue);
                        redisTemplate.delete(virtualQueueLock);
                    }
                }
                return null;
//                lifeExtensionCancel();
            }
        });
        delayTimeoutTaskManager.schedule(task1, startTime);
    }

    private void lifeExtension() {
        if (AckMode.MAUAL.equals(ackMode)) {
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
                                    "redis.call('expire', KEYS[1], 60); " +
                                    "return 1; " +
                                    "end; " +
                                    "return 0;";
                            try {
                                DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(lua, Long.class);
                                List<String> list = new ArrayList<>();
                                list.add(getVirtualQueueLock(virtualQueue));
                                Long execute = redisTemplate.execute(redisScript, list);
//                                redisTemplate.expire(messageId, Duration.ofSeconds(60));
                            } catch (Exception e) {
                                if (isRunning()) {
                                    log.error("lifeExtension  redisTemplate.expire Exception", e);
                                }
                            }
                        }
                    }
                }, 60, 20, TimeUnit.SECONDS);
            }
        }
    }
    // 不做取消的逻辑.在并发量大的时候频繁取消反而消耗性能.
//    private void lifeExtensionCancel() {
//        try {
//            if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
//                boolean cancel = scheduledFuture.cancel(false);
//                scheduledFuture = null;
//            }
//        } catch (Exception e) {
//            log.error("lifeExtensionCancel Exception:", e);
//        }
//    }

//原来在pop()方法下面
//                String script = "local expiredValues = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1], 'limit', 0, ARGV[2]);\n" +
//                        "if #expiredValues > 0 then\n" +
//                        "       local count = redis.call('zrem', KEYS[2], unpack(expiredValues));\n" +
//                        "       if count > 0 then\n" +
//                        "       for i, v in ipairs(expiredValues) do\n" +
//                        "       redis.call('rpush', KEYS[1], v);\n" +
//                        "       end;\n" +
//                        "       end;\n" +
//                        "end;\n" +
//                        "local value = redis.call('zrange', KEYS[2], 0, 0, 'WITHSCORES');\n" +
//                        "if value[1] ~= nil then\n" +
//                        "return value[2];\n" +
//                        "end\n" +
//                        "return nil;";
////        String script="local value = redis.call('LRANGE',KEYS[1],0,9);\n" +
////                "if value  then\n" +
////                "             redis.call('sadd', KEYS[2], value)\n" +
////                "             redis.call('LTRIM',KEYS[1],10,-1);\n" +
////                "return value\n" +
////                "   else\n" +
////                "     return nil\n" +
////                "end;";
//                //低版本用List  高版本要换成获取到的实际的类 就是Message
//                DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(script, Long.class);
//                List<String> list = new ArrayList<>();
//                list.add(getQueueName() + "_delay_list");
//                list.add(getQueueName());
//                return redisTemplate.execute(redisScript, list, System.currentTimeMillis(), 100);
}

