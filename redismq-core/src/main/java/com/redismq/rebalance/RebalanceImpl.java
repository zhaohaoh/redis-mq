package com.redismq.rebalance;


import com.redismq.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.redismq.queue.QueueManager.CURRENT_VIRTUAL_QUEUES;

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
            CURRENT_VIRTUAL_QUEUES.put(queue, vQueues);
        }
        String vQueues = CURRENT_VIRTUAL_QUEUES.entrySet().stream().map(a -> a.getKey() + ":" + a.getValue()).collect(Collectors.joining("\n"));
        log.info("RebalanceImpl rebalance clientId:{}  clientIds:{} vQueues:{}", clientId, clientIds,vQueues);
    }

}
