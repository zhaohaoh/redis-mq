package com.redismq.queue;


import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.redismq.constant.GlobalConstant.SPLITE;

/**
 * @Author: hzh
 * @Date: 2021/11/25 16:03
 */
public class QueueManager {
    public static final Set<String> INVOKE_VIRTUAL_QUEUES = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public static final Map<String, Queue> QUEUES = new LinkedHashMap<>();
    //虚拟队列
    public static final Map<String, List<String>> VIRTUAL_QUEUES = new HashMap<>();
    //当前客户端的虚拟队列  key是真实队列
    public static final Map<String, List<String>> CURRENT_VIRTUAL_QUEUES = new ConcurrentHashMap<>();

    public static int VIRTUAL_QUEUES_NUM;

    public static Queue registerQueue(Queue queue) {
        String queueName = queue.getQueueName();
        Queue returnQueue = QUEUES.computeIfAbsent(queueName, q -> queue);
        if (returnQueue.getVirtual()==null||returnQueue.getVirtual() <= 0) {
            returnQueue.setVirtual(QueueManager.VIRTUAL_QUEUES_NUM);
        }
        List<String> arrayList = new ArrayList<>();
        for (int i = 0; i < returnQueue.getVirtual(); i++) {
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

    public static String getQueueNameByVirtual(String virtual) {
        return StringUtils.substringBeforeLast(virtual, SPLITE);
    }

    public static Queue getQueueByVirtual(String virtual) {
        return getQueue(StringUtils.substringBeforeLast(virtual, SPLITE));
    }
}
