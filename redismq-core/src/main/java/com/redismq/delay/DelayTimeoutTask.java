package com.redismq.delay;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


import static com.redismq.common.constant.StateConstant.RUNNING;
import static com.redismq.common.constant.StateConstant.STOP;
import static com.redismq.delay.DelayTimeoutTaskManager.EXECUTOR;

/**
 * @Author: hzh
 * @Date: 2022/4/25 12:05
 * 延时任务  一个虚拟队列一个任务实例DelayTimeoutTask
 */
public abstract class DelayTimeoutTask {
    protected static final Logger log = LoggerFactory.getLogger(DelayTimeoutTask.class);
    //时间轮
    private final HashedWheelTimer timer = new HashedWheelTimer(new DefaultThreadFactory("REDISMQ-HashedWheelTimer-WORKER"), 100, TimeUnit.MILLISECONDS, 1024, false);
    private final Map<Long, TimeoutTask> timeoutTaskMap = new ConcurrentHashMap<>();
    private volatile int state = RUNNING;

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
        scheduleTask(startTime);
    }

    public void stop() {
        timer.stop();
        state = STOP;
    }

    private void scheduleTask(final Long startTime) {
        if (startTime == null || !isRunning()) {
            //结束本次循环调用
            return;
        }
        //已经存在相同时间的任务
        TimeoutTask timeoutTask = timeoutTaskMap.get(startTime);
        if (timeoutTask!=null){
            return;
        }
        long delay = startTime - System.currentTimeMillis();
      
        if (delay > 10) {
            // 加入时间轮执行
            long l = timer.pendingTimeouts();
            Timeout timeout = timer.newTimeout(t -> {
                try {
                    invokeTask();
                } finally {
                    //任务执行完清理缓存
                    timeoutTaskMap.remove(startTime);
                }
            }, delay, TimeUnit.MILLISECONDS);
            TimeoutTask newTimeTask = new TimeoutTask(startTime, timeout);
            timeoutTaskMap.put(startTime,newTimeTask);
        } else {
            //立即执行
            invokeTask();
        }
    }

    public boolean isRunning() {
        return state == RUNNING;
    }

    protected abstract Set<Long> pullTask();

    private void invokeTask() {
        Runnable runnable = () -> {
           
            if (isRunning()) {
                //执行真正任务
                Set<Long> nextTimes = pullTask();
                if (!CollectionUtils.isEmpty(nextTimes)) {
                    //重复调用
                    for (Long nextTime : nextTimes) {
                        scheduleTask(nextTime);
                    }
                }
            }
        };
        EXECUTOR.execute(runnable);
    }

}

