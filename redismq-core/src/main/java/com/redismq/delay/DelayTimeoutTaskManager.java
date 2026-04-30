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
    
    /**
     * 虚拟队列 -> 调度任务实例。
     *
     * 一个虚拟队列始终只维护一个 DelayTimeoutTask，方便在 rebalance 时做 quiesce / restart。
     */
    private final ConcurrentMap<String, DelayTimeoutTask> tasks = new ConcurrentHashMap<>();
    
    /**
     * 虚拟队列 -> 实际执行 pullTask 的线程池。
     *
     * 这里保留静态缓存，目的是让同一个虚拟队列的调度线程和执行线程在生命周期内复用，
     * 避免频繁创建销毁线程带来的抖动。
     */
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
            // 这里保留 CallerRunsPolicy，是为了“宁可让当前调度线程自己执行，也不要把 pullTask 触发直接丢掉”。
            // 这一层是控制面任务，不适合静默丢弃；同时每个虚拟队列只有自己的调度入口，回压范围是局部可控的。
            }, new ThreadPoolExecutor.CallerRunsPolicy());
            return threadPoolExecutor;
        });
    }
    
    public synchronized void schedule(DelayTimeoutTask task, Long startTime) {
        task.start(startTime);
    }
    
    /**
     * 让某个虚拟队列停止接收新的调度，但保留当前执行中的任务自然结束。
     */
    public synchronized void quiesce(String name) {
        DelayTimeoutTask task = tasks.get(name);
        if (task != null) {
            task.quiesce();
        }
    }

    public synchronized void remove(String name) {
        DelayTimeoutTask task = tasks.get(name);
        if (task != null) {
            task.stop();
            tasks.remove(name, task);
        }
    }
    
    public void stop() {
        // stop 是容器级关闭，所有虚拟队列的调度任务都会被停掉；
        // 执行线程池沿用现有缓存，后续如果同一 JVM 内重新建容器，仍可继续复用。
        tasks.forEach((s, delayTimeoutTask) -> delayTimeoutTask.stop());
        tasks.clear();
    }
}
