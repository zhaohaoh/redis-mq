package com.redismq.queue;


import com.redismq.connection.RedisMQClientUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.redismq.constant.GlobalConstant.SPLITE;

/**
 * @Author: hzh
 * @Date: 2021/11/25 16:03
 */
 
public class QueueManager {
    
    /**
     * 正在拉取消息的队列任务
     */
    public static final Set<String> INVOKE_VIRTUAL_QUEUES = Collections.newSetFromMap(new ConcurrentHashMap<>());
    
    //redis队列  用来决定是否能消息
    public static final Map<String, Queue> REDIS_QUEUES = new LinkedHashMap<>();
    
    //本地队列   用来决定是否能消费消息
    private static final Map<String, Queue> LOCAL_QUEUES = new HashMap<>();
    
    //redis虚拟队列
    private static final Map<String, List<String>> REDIS_VIRTUAL_QUEUES = new HashMap<>();
    
    //本地虚拟队列
    private static final Map<String, List<String>> LOCAL_VIRTUAL_QUEUES = new HashMap<>();
    
    //当前客户端的虚拟队列  key是真实队列  负载均衡后
    private static final Map<String, List<String>> CURRENT_VIRTUAL_QUEUES = new ConcurrentHashMap<>();
    
    public static int VIRTUAL_QUEUES_NUM;
    
    private static RedisMQClientUtil redisMQClientUtil;
    
    @Autowired
    public void setRedisMQClientUtil(RedisMQClientUtil redisMQClientUtil) {
        QueueManager.redisMQClientUtil = redisMQClientUtil;
    }
    
    
    /**
     * 注册本地队列
     *
     * @param queue 队列
     * @return {@link Queue}
     */
    public static Queue registerLocalQueue(Queue queue) {
        String queueName = queue.getQueueName();
        Queue returnQueue = LOCAL_QUEUES.computeIfAbsent(queueName, q -> queue);
        if (returnQueue.getVirtual() == null || returnQueue.getVirtual() <= 0) {
            returnQueue.setVirtual(QueueManager.VIRTUAL_QUEUES_NUM);
        }
        List<String> arrayList = new ArrayList<>();
        for (int i = 0; i < returnQueue.getVirtual(); i++) {
            arrayList.add(returnQueue.getQueueName() + SPLITE + i);
        }
        LOCAL_VIRTUAL_QUEUES.put(returnQueue.getQueueName(), arrayList);
        return returnQueue;
    }
    
    
    /**
     * 获取当地队列
     *
     * @return {@link List}<{@link String}>
     */
    public static List<String> getLocalQueues() {
        return new ArrayList<>(LOCAL_QUEUES.keySet());
    }
    
    /**
     * 获取当地队列
     *
     * @return {@link Map}<{@link String}, {@link Queue}>
     */
    public static Map<String, Queue> getLocalQueueMap() {
        return new HashMap<>(LOCAL_QUEUES);
    }
    
    public static boolean hasSubscribe() {
        return CURRENT_VIRTUAL_QUEUES.size() > 0;
    }
    
    /**
     * 获取虚拟队列
     *
     * @param queue 队列
     * @return {@link List}<{@link String}>
     */
    public static List<String> getLocalVirtualQueues(String queue) {
        return LOCAL_VIRTUAL_QUEUES.get(queue);
    }
    
    /**
     * 获取负载后当前虚拟队列
     *
     * @return {@link Map}<{@link String}, {@link List}<{@link String}>>
     */
    public static Map<String, List<String>> getCurrentVirtualQueues() {
        return CURRENT_VIRTUAL_QUEUES;
    }
    
    /**
     * put当前虚拟队列
     *
     * @param queue   队列
     * @param vQueues v队列
     */
    public static void putCurrentVirtualQueues(String queue, List<String> vQueues) {
        CURRENT_VIRTUAL_QUEUES.put(queue, vQueues);
    }
    
    /**
     * 获取队列
     *
     * @param name 名字
     * @return {@link Queue}
     */
    public static Queue getQueue(String name) {
        Queue queue;
        if (REDIS_QUEUES.get(name) != null) {
            queue = REDIS_QUEUES.get(name);
        } else {
            queue = redisMQClientUtil.getQueue(name);
            if (queue!=null){
                registerLocalQueue(queue);
            }
        }
        return queue;
    }
    
    /**
     * 获取队列名字根据虚拟
     *
     * @param virtual 虚拟
     * @return {@link String}
     */
    public static String getQueueNameByVirtual(String virtual) {
        return StringUtils.substringBeforeLast(virtual, SPLITE);
    }
    
    /**
     * 获取队列根据虚拟
     *
     * @param virtual 虚拟
     * @return {@link Queue}
     */
    public static Queue getQueueByVirtual(String virtual) {
        return getQueue(StringUtils.substringBeforeLast(virtual, SPLITE));
    }
    
    /**
     * 注册redis队列
     *
     * @param queue 队列
     * @return {@link Queue}
     */
    public static Queue registerRedisQueue(Queue queue) {
        String queueName = queue.getQueueName();
        Queue returnQueue = REDIS_QUEUES.computeIfAbsent(queueName, q -> queue);
        if (returnQueue.getVirtual() == null || returnQueue.getVirtual() <= 0) {
            returnQueue.setVirtual(QueueManager.VIRTUAL_QUEUES_NUM);
        }
        List<String> arrayList = new ArrayList<>();
        for (int i = 0; i < returnQueue.getVirtual(); i++) {
            arrayList.add(returnQueue.getQueueName() + SPLITE + i);
        }
        REDIS_VIRTUAL_QUEUES.put(returnQueue.getQueueName(), arrayList);
        return returnQueue;
    }
}
