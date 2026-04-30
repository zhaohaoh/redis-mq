package com.redismq.delay;

import com.redismq.common.connection.RedisMQClientUtil;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.redismq.common.config.GlobalConfigCache.GLOBAL_CONFIG;
import static com.redismq.common.constant.RedisMQConstant.getVirtualQueueLock;
import static com.redismq.common.constant.StateConstant.RUNNING;
import static com.redismq.common.constant.StateConstant.STOP;

/**
 * @Author: hzh
 * @Date: 2022/4/25 12:05 延时任务  一个虚拟队列一个任务实例DelayTimeoutTask
 */
public abstract class DelayTimeoutTask {
    
    protected static final Logger log = LoggerFactory.getLogger(DelayTimeoutTask.class);
    /**
     * 抢锁失败后的兜底重试间隔。
     *
     * rebalance 后新 owner 可能已经把虚拟队列重新入队，但旧 owner 的锁还没自然过期；
     * 这里不能“一次抢不到就放弃”，否则那条分片会静默停住，必须定时再试一次。
     */
    private static final long LOCK_RETRY_MILLIS = 1000L;
    
    //时间轮
    private final HashedWheelTimer timer = new HashedWheelTimer(
            new DefaultThreadFactory("REDISMQ-HashedWheelTimer-WORKER"), 100, TimeUnit.MILLISECONDS, 1024, false);
    
    private final Map<Long, TimeoutTask> timeoutTaskMap = new ConcurrentHashMap<>();
    
    private volatile int state = RUNNING;
    /**
     * 是否还接受新的调度请求。
     *
     * 和 stop 的区别：
     * 1. quiesce 只是不再接新任务，但允许当前已在执行的 pullTask 自然收尾；
     * 2. stop 是容器级关闭，除了停止调度，还会停止时间轮并释放锁。
     */
    private volatile boolean acceptingNewTasks = true;
    
    private final String virtualQueue;
    
    private final RedisMQClientUtil redisMQClientUtil;
    
    public DelayTimeoutTask(String virtualQueue, RedisMQClientUtil redisMQClientUtil) {
        this.virtualQueue = virtualQueue;
        this.redisMQClientUtil = redisMQClientUtil;
   
    }
    
    public static class TimeoutTask {
        
        private final long startTime;
        
        private final Timeout task;
        
        public TimeoutTask(long startTime, Timeout task) {
            super();
            this.startTime = startTime;
            this.task = task;
        }
        
        public long getStartTime() {
            return startTime;
        }
        
        public Timeout getTask() {
            return task;
        }
        
    }
    
    /**
     * 启动或重新激活当前虚拟队列的调度任务。
     *
     * 同一个虚拟队列在 rebalance 后可能重新分配回本节点，因此 start 不能只在首次创建时可用，
     * 还要能把 quiesce 之后的实例重新切回“允许接新任务”的状态。
     */
    public void start(Long startTime) {
        if (state == STOP) {
            return;
        }
        acceptingNewTasks = true;
        scheduleTask(startTime,false);
    }
    
    /**
     * 进入渐进下线状态。
     *
     * 这里只取消未来的定时触发，不主动释放当前虚拟队列锁。
     * 原因是锁仍然要保护正在执行中的消息，直到 pull 线程自己跑完并退出。
     */
    public void quiesce() {
        acceptingNewTasks = false;
        timeoutTaskMap.forEach((key, task) -> task.getTask().cancel());
        timeoutTaskMap.clear();
    }

    /**
     * 完整停止任务。
     *
     * 和 quiesce 不同，stop 用于容器关闭或彻底暂停，需要停止时间轮并释放锁。
     */
    public void stop() {
        state = STOP;
        acceptingNewTasks = false;
        timeoutTaskMap.forEach((key, task) -> task.getTask().cancel());
        timer.stop();
        timeoutTaskMap.clear();
        // 虚拟队列锁定执行任务默认时长,有看门狗机制
        String virtualQueueLock = getVirtualQueueLock(virtualQueue);
        // 释放锁
        unLockQueue(virtualQueueLock);
    }
    
    private void scheduleTask(final Long startTime,boolean forceLock) {
        if (startTime == null || !isRunning() || (!acceptingNewTasks && !forceLock)) {
            //结束本次循环调用
            return;
        }
        //已经存在相同时间的任务
        TimeoutTask timeoutTask = timeoutTaskMap.get(startTime);
        if (timeoutTask != null) {
            return;
        }
        long delay = startTime - System.currentTimeMillis();
        
        if (delay > 10) {
            // 未来时间点的触发交给时间轮；相同 startTime 只保留一个 timeout，避免重复调度。
            Timeout timeout = timer.newTimeout(t -> {
                try {
                  invokeTask(false);
                }finally {
                    //任务执行完清理缓存
                    timeoutTaskMap.remove(startTime);
                }
            }, delay, TimeUnit.MILLISECONDS);
            TimeoutTask newTimeTask = new TimeoutTask(startTime, timeout);
            timeoutTaskMap.put(startTime, newTimeTask);
        } else {
            // 已经过触发时间或者需要立即补拉时，直接走执行逻辑。
            invokeTask(forceLock);
        }
    }
    
    public boolean isRunning() {
        return state == RUNNING;
    }
    
    protected abstract Set<Long> pullTask();
    
    private boolean invokeTask(boolean forceLock) {
        if (!forceLock && !acceptingNewTasks) {
            return false;
        }
        // 虚拟队列锁定执行任务默认时长,有看门狗机制
        String virtualQueueLock = getVirtualQueueLock(virtualQueue);
        
     
        if (!forceLock){
            // 这里被锁住,如果有服务下线了.需要超过这个时间消息才能继续被消费.因为会被锁定.
            Boolean success = lockQueue(virtualQueueLock);
            if (GLOBAL_CONFIG.printConsumeLog) {
                log.info("current virtualQueue:{} tryLockSuccess:{} state:{}", virtualQueue, success, state);
            }
    
            //获取锁失败
            if (success == null || !success) {
                // 抢锁失败不代表当前分片永远不可消费。
                // 典型场景是 rebalance 刚完成，但旧 owner 还在处理最后一批消息，这时需要稍后再尝试接管。
                if (acceptingNewTasks && isRunning()) {
                    scheduleTask(System.currentTimeMillis() + LOCK_RETRY_MILLIS, false);
                }
                return false;
            }
        }
        
        Runnable runnable = () -> {
            try {
                if (isRunning()) {
                    //执行真正任务
                    Set<Long> nextTimes = pullTask();
                    if (acceptingNewTasks && !CollectionUtils.isEmpty(nextTimes)) {
                        // pullTask 返回的是“下一次需要被唤醒的时间点”。
                        // 这里仍然统一回到 scheduleTask，由调度层决定是立即执行还是进时间轮。
                        //重复调用
                        for (Long nextTime : nextTimes) {
                            if (isRunning() && acceptingNewTasks) {
                                scheduleTask(nextTime,false);
                            }
                        }
                    }
                }
            } finally {
                unLockQueue(virtualQueueLock);
            }
        };
        try {
            DelayTimeoutTaskManager.computeIfAbsent(virtualQueue).execute(runnable);
        } catch (Exception e) {
            // 提交线程池失败时必须解锁，否则这个虚拟队列会被锁住却没人真正处理。
            log.info("execute task unLockQueue error: ", e);
            unLockQueue(virtualQueueLock);
        }
        return true;
    }
    
    //获取锁和释放锁的动作只能有一个线程执行 后面要优化成redisson
    public void unLockQueue(String virtualQueueLock) {
        redisMQClientUtil.unlock(virtualQueueLock);
    }
    
    //获取锁和释放锁的动作只能有一个线程执行 后面要优化成redisson
    public Boolean lockQueue(String virtualQueueLock) {
     
        int i = 0;
        Boolean lock = redisMQClientUtil.lock(virtualQueueLock, Duration.ofSeconds(GLOBAL_CONFIG.virtualLockTime));
       
        while (!lock && i < 2) {
            i++;
            try {
                Thread.sleep(300L);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
            lock = redisMQClientUtil.lock(virtualQueueLock, Duration.ofSeconds(GLOBAL_CONFIG.virtualLockTime));
        }
        return lock;
    }
    
}

