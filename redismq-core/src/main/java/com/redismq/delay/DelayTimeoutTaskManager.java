package com.redismq.delay;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @Author: hzh
 * @Date: 2022/4/25 12:30
 * 延时任务执行管理器
 */
public class DelayTimeoutTaskManager {
    private final ConcurrentMap<String, DelayTimeoutTask> tasks = new ConcurrentHashMap<>();
    public static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(5, 5,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(10000), new ThreadFactory() {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private static final String NAME_PREFIX = "REDISMQ-PULLTASK-EXECUTOR-";

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
    }, new ThreadPoolExecutor.CallerRunsPolicy());
    
    public DelayTimeoutTask computeIfAbsent(String name, Function<? super String, ? extends DelayTimeoutTask> mappingFunction) {
        return tasks.computeIfAbsent(name, mappingFunction);
    }

    public synchronized void schedule(DelayTimeoutTask task, Long startTime) {
        task.start(startTime);
    }

    public synchronized void remove(String name) {
        DelayTimeoutTask task = tasks.get(name);
        if (task != null) {
            task.stop();
            tasks.remove(name, task);
        }
    }

    public void stop() {
        tasks.forEach((s, delayTimeoutTask) ->delayTimeoutTask.stop());
        tasks.clear();
    }
}
