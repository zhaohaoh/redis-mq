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
import org.springframework.util.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hzh
 * @date 2021/8/10
 * redis延时队列实现   通过发布订阅和时间轮实现高性能。  一个queue对应一个端点对应多个queue:tag
 */
public class RedisMQListenerContainer extends AbstractMessageListenerContainer {
    protected static final Logger log = LoggerFactory.getLogger(RedisMQListenerContainer.class);
    private final DelayTimeoutTaskManager delayTimeoutTaskManager = new DelayTimeoutTaskManager();
    private final ThreadPoolExecutor boss = new ThreadPoolExecutor(1, 1,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactory() {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private static final String NAME_PREFIX = "redis-mq-delay-boss-";

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

    private final ThreadPoolExecutor work = new ThreadPoolExecutor(getConcurrency(), getMaxConcurrency(),
            60L, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(100), new ThreadFactory() {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private static final String NAME_PREFIX = "redis-mq-delay-work-";

        {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, NAME_PREFIX + threadNumber.getAndIncrement());
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    });


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
    }

    public Set<Long> pop(String queueName) {
        Set<Long> startTimeSet = new HashSet<>();
        while (isRunning()) {
            try {
                //获取已经到时间要执行的任务  本地消息的数量相当于本地偏移量   localMessages.size()是指从这个位置之后开始啦
                long pullTime = System.currentTimeMillis();
                Set<ZSetOperations.TypedTuple<Object>> tuples = redisTemplate.opsForZSet().rangeByScoreWithScores(queueName, 0, pullTime, localMessages.size(), super.maxConcurrency);
                if (CollectionUtils.isEmpty(tuples)) {
                    //本地消息没有消费完就先不取延时任务的.
                    if (localMessages.size() > 0) {
                        break;
                    }
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
                    break;
                }
                for (ZSetOperations.TypedTuple<Object> tuple : tuples) {
                    Message value = (Message) tuple.getValue();
                    try {
                        semaphore.acquire();
                    } catch (InterruptedException e) {
                        log.info("redismq acquire semaphore InterruptedException", e);
                        if (isRunning()) {
                            log.error("redismq acquire semaphore error", e);
                        }
                        Thread.currentThread().interrupt();
                    }
                    //手动ack
                    if (AckMode.MAUAL.equals(ackMode)) {
                        boolean success = localMessages.add(value);
                        if (!success) {
                            continue;
                        }
                    } else {
                        redisTemplate.opsForZSet().remove(queueName, value);
                    }
                    try {
                        String id = super.getRunableKey(value.getTag());
                        RedisListenerRunnable runnable = super.createRedisListenerRunnable(id, value);
                        if (runnable == null) {
                            throw new RedisMqException("redismq not found tag runnable");
                        }
                        // 多线程执行完毕后semaphore.release();
                        work.execute(runnable);
                    } catch (Exception e) {
                        //如果跑错直接释放资源，否则线程执行完毕才释放
                        log.error("redismq listener container error  semaphore:{}", semaphore.toString(), e);
                        semaphore.release();
                    }
                }
            } catch (Exception e) {
                //报错需要  semaphore.release();
                log.error("redismq pop error", e);
                if (e.getMessage().contains("WRONGTYPE Operation against a key holding the wrong kind of value")) {
                    log.error("redismq [ERROR] queue not is zset type。 cancel pop");
                    stop();
                }
//                if (e instanceof ClassCastException){
//                    log.error("redismq [ERROR] ClassCastException",e);
//                    stop();
//                }
                semaphore.release();
            }
        }
        return startTimeSet;
    }


    @Override
    public void repush() {
        throw new RedisMqException("延时队列不存在的方法  repush()");
    }


    public void start(Long startTime) {
        //为空说明当前能获取到数据
        DelayTimeoutTask task1 = delayTimeoutTaskManager.computeIfAbsent(queueName, task -> new DelayTimeoutTask() {
            @Override
            protected Set<Long> pullTask() {
                List<String> virtualQueues = QueueManager.CURRENT_VIRTUAL_QUEUES.get(queueName);
                if (CollectionUtils.isEmpty(virtualQueues)) {
                    return null;
                }
                Set<Long> nextTimeSet = new HashSet<>();
                while (true) {
                    int i = 0;
                    for (String virtualQueue : virtualQueues) {
                        Set<Long> pop = pop(virtualQueue);
                        nextTimeSet.addAll(pop);
                        //为空说明当前能获取到数据
                        if (CollectionUtils.isEmpty(pop)) {
                            i++;
                        }
                    }
                    if (i >= virtualQueues.size()) {
                        return nextTimeSet;
                    }
                }
            }
        });
        delayTimeoutTaskManager.schedule(task1, startTime);
    }


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

