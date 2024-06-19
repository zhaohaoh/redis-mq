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
import java.util.concurrent.atomic.AtomicReference;

import static com.redismq.common.config.GlobalConfigCache.GLOBAL_CONFIG;
import static com.redismq.common.constant.RedisMQConstant.getVirtualQueueLock;
import static com.redismq.common.constant.StateConstant.RUNNING;
import static com.redismq.common.constant.StateConstant.STOP;
import static com.redismq.delay.DelayTimeoutTaskManager.EXECUTOR;

/**
 * @Author: hzh
 * @Date: 2022/4/25 12:05 延时任务  一个虚拟队列一个任务实例DelayTimeoutTask
 */
public abstract class DelayTimeoutTask {
    
    protected static final Logger log = LoggerFactory.getLogger(DelayTimeoutTask.class);
    
    //时间轮
    private final HashedWheelTimer timer = new HashedWheelTimer(
            new DefaultThreadFactory("REDISMQ-HashedWheelTimer-WORKER"), 100, TimeUnit.MILLISECONDS, 1024, false);
    
    private final Map<Long, TimeoutTask> timeoutTaskMap = new ConcurrentHashMap<>();
    
    private volatile int state = RUNNING;
    
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
    
    private final AtomicReference<TimeoutTask> lastTimeoutTask = new AtomicReference<>();
    
    public void start(Long startTime) {
        scheduleTask(startTime,false);
    }
    
    public void stop() {
        state = STOP;
        timer.stop();
        timeoutTaskMap.clear();
        // 虚拟队列锁定执行任务默认时长,有看门狗机制
        String virtualQueueLock = getVirtualQueueLock(virtualQueue);
        // 释放锁
        unLockQueue(virtualQueueLock);
    }
    
    private void scheduleTask(final Long startTime,boolean forceLock) {
        if (startTime == null || !isRunning()) {
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
            // 加入时间轮执行
            long l = timer.pendingTimeouts();
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
            //立即执行
            invokeTask(forceLock);
        }
    }
    
    public boolean isRunning() {
        return state == RUNNING;
    }
    
    protected abstract Set<Long> pullTask();
    
    private boolean invokeTask(boolean forceLock) {
        // 虚拟队列锁定执行任务默认时长,有看门狗机制
        String virtualQueueLock = getVirtualQueueLock(virtualQueue);
        
        //如果是强制锁直接执行方法
        if (!forceLock){
            // 这里被锁住,如果有服务下线了.需要超过这个时间消息才能继续被消费.因为会被锁定.
            Boolean success = lockQueue(virtualQueueLock);
            if (GLOBAL_CONFIG.printConsumeLog) {
                log.info("current virtualQueue:{} tryLockSuccess:{} state:{}", virtualQueue, success, state);
            }
    
            //获取锁失败
            if (success == null || !success) {
                 return false;
            }
        }
        
        Runnable runnable = () -> {
            try {
                if (isRunning()) {
                    //执行真正任务
                    Set<Long> nextTimes = pullTask();
                    if (!CollectionUtils.isEmpty(nextTimes)) {
                        //重复调用
                        for (Long nextTime : nextTimes) {
                            if (isRunning()) {
                                scheduleTask(nextTime,true);
                            }
                        }
                    }
                }
            } finally {
                unLockQueue(virtualQueueLock);
            }
        };
        try {
            EXECUTOR.execute(runnable);
        } catch (Exception e) {
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
                Thread.sleep(200L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock = redisMQClientUtil.lock(virtualQueueLock, Duration.ofSeconds(GLOBAL_CONFIG.virtualLockTime));
        }
        return lock;
    }
    
}

