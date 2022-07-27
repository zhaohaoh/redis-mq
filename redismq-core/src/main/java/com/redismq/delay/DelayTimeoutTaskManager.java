package com.redismq.delay;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * @Author: hzh
 * @Date: 2022/4/25 12:30
 * 延时任务执行管理器
 */
public class DelayTimeoutTaskManager {
    private final ConcurrentMap<String, DelayTimeoutTask> tasks = new ConcurrentHashMap<>();

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
