package com.redismq.queue;


import com.redismq.constant.RedisMQConstant;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.redismq.constant.QueueConstant.SPLITE;

/**
 * @Author: hzh
 * @Date: 2021/11/25 16:03
 */
public class QueueManager {
    public static final Map<String, Queue> QUEUES = new LinkedHashMap<>();
    //虚拟队列
    public static final Map<String, List<String>> VIRTUAL_QUEUES = new HashMap<>();
    //当前客户端的虚拟队列
    public static final Map<String, List<String>> CURRENT_VIRTUAL_QUEUES = new ConcurrentHashMap<>();

    public static int VIRTUAL_QUEUES_NUM;

    public static Queue registerQueue(Queue queue) {
        queue.setQueueName(RedisMQConstant.getQueueName(queue.getQueueName()));
        String queueName = queue.getQueueName();
        Queue returnQueue = QUEUES.computeIfAbsent(queueName, q -> queue);
        returnQueue.setVirtual(QueueManager.VIRTUAL_QUEUES_NUM);
        List<String> arrayList = new ArrayList<>();
        for (int i = 0; i < VIRTUAL_QUEUES_NUM; i++) {
            arrayList.add(returnQueue.getQueueName() + SPLITE + i);
        }
        VIRTUAL_QUEUES.put(returnQueue.getQueueName(), arrayList);
        return returnQueue;
    }


    public static List<String> getAllQueues() {
        return new ArrayList<>(QUEUES.keySet());
    }

    public static Map<String, Queue> getAllQueueMap() {
        return new HashMap<>(QUEUES);
    }

    public static boolean hasSubscribe() {
        return CURRENT_VIRTUAL_QUEUES.size() > 0;
    }

    public static List<String> getVirtualQueues(String queue) {
        return VIRTUAL_QUEUES.get(queue);
    }

    public static Queue getQueue(String name) {
        return QUEUES.get(name) != null ? QUEUES.get(name) : null;
    }
}
