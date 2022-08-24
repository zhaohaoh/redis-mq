package com.redismq.delay;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.redismq.delay.DelayTimeoutTaskManager.EXECUTOR;

/**
 * @Author: hzh
 * @Date: 2022/4/25 12:05
 * 延时任务
 */
public abstract class DelayTimeoutTask {
    protected static final Logger log = LoggerFactory.getLogger(DelayTimeoutTask.class);
    //时间轮
    private final HashedWheelTimer timer = new HashedWheelTimer(new DefaultThreadFactory("REDISMQ-HashedWheelTimer-WORKER"), 100, TimeUnit.MILLISECONDS, 1024, false);
    private final Map<Long, TimeoutTask> timeoutTaskMap = new ConcurrentHashMap<>();

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
    }

    private void scheduleTask(final Long startTime) {
        if (startTime == null) {
            //结束本次循环调用
            return;
        }
        TimeoutTask oldTimeout = lastTimeoutTask.get();

        if (oldTimeout != null) {
            oldTimeout.getTask().cancel();
        }

        long delay = startTime - System.currentTimeMillis();
        if (delay > 10) {
            // 加入时间轮执行
            Timeout timeout = timer.newTimeout(t -> {
                try {
                    invokeTask();
                } finally {
                    //任务执行完清理缓存
                    timeoutTaskMap.remove(startTime);
                }
                TimeoutTask currentTimeout = lastTimeoutTask.get();
                //任务执行完设置执行器为空
                if (currentTimeout.getTask() == t) {
                    lastTimeoutTask.compareAndSet(currentTimeout, null);
                }
            }, delay, TimeUnit.MILLISECONDS);
            TimeoutTask task = timeoutTaskMap.computeIfAbsent(startTime, timeoutTask -> new TimeoutTask(startTime, timeout));
            //替换旧任务为新任务失败的话.取消上面的延时任务
            if (!lastTimeoutTask.compareAndSet(oldTimeout, task)) {
                timeout.cancel();
            }
        } else {
            //立即执行
            invokeTask();
        }
    }

    protected abstract Set<Long> pullTask();

    private void invokeTask() {
        EXECUTOR.execute(() -> {
            //执行真正任务
            Set<Long> nextTimes = pullTask();
            if (!CollectionUtils.isEmpty(nextTimes)) {
                //重复调用
                for (Long nextTime : nextTimes) {
                    scheduleTask(nextTime);
                }
            }
        });
    }

}