package com.redismq.delay;

import com.redismq.common.pojo.Queue;
import com.redismq.queue.QueueManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @Author: hzh
 * @Date: 2022/4/25 12:30 延时任务执行管理器
 */
public class DelayTimeoutTaskManager {
    
    private final ConcurrentMap<String, DelayTimeoutTask> tasks = new ConcurrentHashMap<>();
    
    public  static final ConcurrentMap<String, ThreadPoolExecutor> THREADPOOLEXECUTOR_MAP = new ConcurrentHashMap<>();
    
    public DelayTimeoutTask computeIfAbsent(String name,
            Function<? super String, ? extends DelayTimeoutTask> mappingFunction) {
        return tasks.computeIfAbsent(name, mappingFunction);
    }
    public static ThreadPoolExecutor computeIfAbsent(String name) {
        
        return THREADPOOLEXECUTOR_MAP.computeIfAbsent(name, a->{
            Queue queue = QueueManager.getQueueByVirtual(name);
            Integer virtual = queue.getVirtual();
            String queueName = queue.getQueueName();
            ThreadPoolExecutor  threadPoolExecutor = new ThreadPoolExecutor(virtual, virtual,
                    60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1000), new ThreadFactory() {
                private final ThreadGroup group;
                private final AtomicInteger threadNumber = new AtomicInteger(1);
                private static final String NAME_PREFIX = "REDISMQ-PULLTASK-EXECUTOR-";
                
                {
                    SecurityManager s = System.getSecurityManager();
                    group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
                }
                
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(group, r, NAME_PREFIX +queueName+"-"+ threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    if (t.getPriority() != Thread.NORM_PRIORITY) {
                        t.setPriority(Thread.NORM_PRIORITY);
                    }
                    return t;
                }
            }, new ThreadPoolExecutor.CallerRunsPolicy());
            return threadPoolExecutor;
        });
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
        tasks.forEach((s, delayTimeoutTask) -> delayTimeoutTask.stop());
        tasks.clear();
    }
}
