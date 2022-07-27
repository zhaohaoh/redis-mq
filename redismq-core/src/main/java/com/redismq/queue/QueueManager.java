package com.redismq.queue;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.redismq.constant.QueueConstant.SPLITE;

/**
 * @Author: hzh
 * @Date: 2021/11/25 16:03
 */
@Component
public class QueueManager {
    public static final Map<String, Queue> QUEUES = new LinkedHashMap<>();
    //虚拟队列
    public static final Map<String, List<String>> VIRTUAL_QUEUES = new HashMap<>();
    //当前客户端的虚拟队列
    public static final Map<String, List<String>> CURRENT_VIRTUAL_QUEUES = new ConcurrentHashMap<>();

    public static int VIRTUAL_QUEUES_NUM;

    @Value("${spring.redismq.virtual:1}")
    public void setVirtualNum(int virtualNum) {
        QueueManager.VIRTUAL_QUEUES_NUM = virtualNum;
        QUEUES.forEach((k, v) -> {
            if (v.getVirtual() == null) {
                v.setVirtual(virtualNum);
            }
            List<String> arrayList = new ArrayList<>();
            for (int i = 0; i < VIRTUAL_QUEUES_NUM; i++) {
                arrayList.add(k + SPLITE + i);
            }
            VIRTUAL_QUEUES.put(k, arrayList);
        });
    }

    public static Queue registerQueue(Queue queue) {
        String queueName = queue.getQueueName();
        return QUEUES.computeIfAbsent(queueName, q -> queue);
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

//    private static void verifyProperties(Queue queue, Queue thisQueue) {
//        if (!queue.getConcurrency().equals(thisQueue.getConcurrency())) {
//            throw new RedisMqException("[redismq 非法配置] 相同的队列不同的配置参数");
//        }
//        if (!queue.getMaxConcurrency().equals(thisQueue.getMaxConcurrency())) {
//            throw new RedisMqException("[redismq 非法配置] 相同的队列不同的配置参数");
//        }
//        if (!queue.getRetryMax().equals(thisQueue.getRetryMax())) {
//            throw new RedisMqException("[redismq 非法配置] 相同的队列不同的配置参数");
//        }
//        if (!queue.getAckMode().equals(thisQueue.getAckMode())) {
//            throw new RedisMqException("[redismq 非法配置] 相同的队列不同的配置参数");
//        }
//        if (thisQueue.getRetryMax() > 0 && AckMode.AUTO.equals(thisQueue.getAckMode())) {
//            throw new RedisMqException("只有手动ack才可以设置重试次数");
//        }
//    }

    public static boolean matchQueue(String name) {
        return QUEUES.containsKey(name);
    }


    public static Queue getQueue(String name) {
        return QUEUES.get(name) != null ? QUEUES.get(name) : null;
    }
}
