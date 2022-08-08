package com.redismq.rebalance;


import com.redismq.queue.QueueManager;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Author: hzh
 * @Date: 2022/4/25 18:18
 * 队列负载均衡相关
 */
public class RebalanceImpl {
    protected static final Logger log = LoggerFactory.getLogger(RebalanceImpl.class);
    private final AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    public RebalanceImpl(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    /*
        返回监听的队列
     */
    public void rebalance(Set<String> clientIds, String clientId) {
        if (CollectionUtils.isEmpty(clientIds)) {
            log.error("rebalance get clients is empty");
            return;
        }
        List<String> allQueues = QueueManager.getAllQueues();
        for (String queue : allQueues) {
            List<String> virtualQueues = QueueManager.getVirtualQueues(queue);
            List<String> vQueues = allocateMessageQueueStrategy.allocate(clientId, virtualQueues, new ArrayList<>(clientIds));
            QueueManager.CURRENT_VIRTUAL_QUEUES.put(queue, vQueues);
            log.info("RebalanceImpl rebalance vQueues:{} clientIds:{}", vQueues, clientIds);
        }
    }

    public static void main(String[] args) {
        List<String> virtualQueues = new ArrayList<>();
        virtualQueues.add("172.31.23.79");


        List<String> virtualQueues1 = new ArrayList<>();
        virtualQueues1.add("172.31.23.79");
        virtualQueues1.add("10.10.0.22");
        AllocateMessageQueueAveragely allocateMessageQueueAveragely = new AllocateMessageQueueAveragely();
        List<String> vQueues = allocateMessageQueueAveragely.allocate("10.10.0.22", virtualQueues, new ArrayList<>(virtualQueues1));
        System.out.println(vQueues);
    }


}
